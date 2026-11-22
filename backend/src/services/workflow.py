import hashlib
import uuid
from datetime import UTC, datetime

from fastapi import HTTPException
from sqlalchemy import select, text

from src.config.settings import cache, engine
from src.constants.workflow import Status
from src.models.tables import audit, cases, evidence, runs
from src.services import domain, schedule
from src.utils.encoding import canonical
from src.utils.time import utc_time

# A running attempt keeps its months until it either finishes or this lease
# expires. An expired attempt is only taken over after its execution lock is
# observed to be free, i.e. the old session really stopped touching the database.
LEASE_SECONDS = 600


def log(conn, actor, action, reference):
    conn.execute(audit.insert().values(actor=actor, action=action, reference=str(reference), created_at=utc_time()))


def recent(table):
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(select(table).order_by(table.c.id.desc()).limit(100)).mappings()]


def found(conn, table, identity):
    row = conn.execute(select(table).where(table.c.id == identity)).mappings().first()
    if row is None:
        raise HTTPException(404, '找不到该记录')
    return dict(row)


def _month_labels(payload):
    # Best effort: an invalid payload still enqueues and fails inside the worker,
    # where the validation error is surfaced as a failed run.
    try:
        return domain.months(payload)
    except (ValueError, TypeError, KeyError):
        return []


def make_case(title, payload, actor):
    title = title.strip()
    if not title:
        raise HTTPException(422, '档案名称不能为空')
    try:
        if len(canonical(payload)) > 512 * 1024:
            raise ValueError('业务输入超过 512 KiB')
        if callable(getattr(domain, 'validate', None)):
            domain.validate(payload)
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, str(exc)) from None
    with engine.begin() as conn:
        case_id = conn.execute(cases.insert().values(slug=uuid.uuid4().hex, title=title, payload=payload,
                               author=actor, created_at=utc_time())).inserted_primary_key[0]
        log(conn, actor, 'create_case', case_id)
        return found(conn, cases, case_id)


def enqueue(case_id, actor):
    with engine.begin() as conn:
        case = found(conn, cases, case_id)
        run_id = conn.execute(runs.insert().values(case_id=case_id, status=Status.QUEUED,
                              owner=actor, months=_month_labels(case['payload']),
                              created_at=utc_time())).inserted_primary_key[0]
        log(conn, actor, 'enqueue', run_id)
        result = found(conn, runs, run_id)
    wake()
    return result


def wake():
    try:
        cache.lpush('worker:wake', 'queued')
        cache.ltrim('worker:wake', 0, 99)
    except Exception:
        # The durable database queue is polled even if Redis is temporarily unavailable.
        pass


def _lease_alive(started_at):
    if not started_at:
        return False
    try:
        started = datetime.fromisoformat(started_at)
    except (TypeError, ValueError):
        return False
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    return (datetime.now(UTC) - started).total_seconds() < LEASE_SECONDS


def _execution_lock_free(conn, run_id):
    # Probe with a transaction-scoped lock: it shares the advisory-lock space
    # with the worker's session lock, fails while the old attempt still holds
    # it, and is released automatically when this claim transaction ends.
    key = schedule.execution_key(run_id)
    return bool(conn.execute(text('SELECT pg_try_advisory_xact_lock(CAST(:key AS bigint))'), {'key': key}).scalar_one())


def _touched_months(stored, payload):
    if isinstance(stored, list) and stored:
        return tuple(stored)
    return tuple(_month_labels(payload))


def claim():
    """Admit one run, or none, under the month-scoped scheduling rules.

    The whole pass runs under a transaction-scoped advisory lock, so the
    snapshot, the recovery checks and the status transitions are atomic with
    respect to every other worker slot.
    """
    with engine.begin() as conn:
        conn.execute(text('SELECT pg_advisory_xact_lock(CAST(:key AS bigint))'), {'key': schedule.SCHEDULER_KEY})
        rows = conn.execute(
            select(runs.c.id, runs.c.status, runs.c.months, runs.c.started_at,
                   cases.c.payload)
            .join(cases, cases.c.id == runs.c.case_id)
            .where(runs.c.status.in_([Status.QUEUED, Status.WAITING, Status.RUNNING]))
            .order_by(runs.c.id)).mappings().all()

        candidates = []
        payloads = {}
        previous = {}
        for row in rows:
            months = _touched_months(row['months'], row['payload'])
            payloads[row['id']] = row['payload']
            previous[row['id']] = str(row['status'])
            candidate = schedule.Candidate(
                id=row['id'], status=str(row['status']), months=months,
                lease_alive=_lease_alive(row['started_at']))
            if candidate.status == schedule.RUNNING and not candidate.lease_alive:
                candidate = schedule.Candidate(
                    id=row['id'], status=candidate.status, months=months,
                    lease_alive=False, lock_free=_execution_lock_free(conn, row['id']))
            candidates.append(candidate)

        decision = schedule.decide(candidates)

        for run_id in decision.recovered:
            conn.execute(runs.update().where(runs.c.id == run_id)
                         .values(status=Status.QUEUED, attempt_token=None, started_at=None, waiting_for=None))
            log(conn, 'worker', 'recover', run_id)

        granted = None
        for candidate in candidates:
            run_id = candidate.id
            state = decision.states[run_id]
            waiting = list(decision.waiting.get(run_id, ()))
            old_status = previous[run_id]
            if state == schedule.WAITING:
                conn.execute(runs.update().where(runs.c.id == run_id)
                             .values(status=Status.WAITING, waiting_for=waiting))
                if old_status != Status.WAITING:
                    log(conn, 'worker', 'wait', run_id)
            elif state == schedule.QUEUED:
                conn.execute(runs.update().where(runs.c.id == run_id)
                             .values(status=Status.QUEUED, waiting_for=None))
            elif run_id == decision.granted:
                token = str(uuid.uuid4())
                conn.execute(runs.update().where(runs.c.id == run_id).values(
                    status=Status.RUNNING, started_at=utc_time(), attempt_token=token, waiting_for=None))
                log(conn, 'worker', 'grant', run_id)
                granted = (run_id, token, payloads[run_id])

        return granted


def has_ready():
    """Whether any run is queued and not waiting, so idle slots can race ahead
    to claim a disjoint batch instead of sleeping until the wake signal."""
    with engine.connect() as conn:
        return conn.execute(select(runs.c.id).where(runs.c.status == Status.QUEUED)
                            .limit(1)).first() is not None


def requeue(run_id, token):
    """Hand a granted run back to the queue without publishing anything.

    Used when the execution lock is unexpectedly busy; the token fence means a
    late finish from the previous attempt cannot overwrite whatever runs next.
    """
    with engine.begin() as conn:
        conn.execute(runs.update().where(runs.c.id == run_id, runs.c.attempt_token == token,
                                         runs.c.status == Status.RUNNING)
                     .values(status=Status.QUEUED, attempt_token=None, started_at=None))
    wake()


def finish(run_id, token, result=None, error=None):
    digest = hashlib.sha256(canonical(result)).hexdigest() if result is not None else None
    status = Status.FAILED if error else Status.COMPLETE
    with engine.begin() as conn:
        # Fencing: only the attempt that currently owns the run may publish a
        # result. A late finish from a superseded attempt matches no row, so its
        # result can never enter the evidence table.
        count = conn.execute(runs.update().where(runs.c.id == run_id,
                        runs.c.attempt_token == token, runs.c.status == Status.RUNNING)
                        .values(status=status, finished_at=utc_time(), error=error)).rowcount
        if not count:
            return
        if result is not None:
            conn.execute(evidence.insert().values(run_id=run_id, report=result, digest=digest))
        log(conn, 'worker', status.value, run_id)
    wake()


def review(identity, note, actor):
    note = note.strip()
    if not note:
        raise HTTPException(422, '复核意见不能为空')
    with engine.begin() as conn:
        found(conn, evidence, identity)
        count = conn.execute(evidence.update().where(evidence.c.id == identity, evidence.c.reviewed_at.is_(None))
                            .values(review_note=note, reviewer=actor, reviewed_at=utc_time())).rowcount
        if count != 1:
            raise HTTPException(409, '该证据已经复核')
        log(conn, actor, 'review', identity)
        return found(conn, evidence, identity)
