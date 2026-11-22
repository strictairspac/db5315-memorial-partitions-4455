import logging
import os
from pathlib import Path
from threading import Event, Thread
from time import sleep

from sqlalchemy import text

from src.config.settings import ARTIFACTS, cache, engine
from src.services.domain import solve
from src.services.schedule import execution_key
from src.services.workflow import claim, finish, has_ready, requeue

HEARTBEAT = Path(ARTIFACTS) / 'worker-heartbeat'
# Independent slots let batches on disjoint months run at the same time. The
# scheduler still admits at most one run per pass, so slots never overlap months.
SLOTS = max(1, int(os.getenv('WORKER_SLOTS', '4')))


def heartbeat(stop):
    while not stop.is_set():
        HEARTBEAT.touch()
        stop.wait(2)


def execute(run_id, token, payload):
    """Run one batch while pinning its execution lock.

    The lock is held on a dedicated connection for the whole database phase and
    released only after the result (or failure) is published. A successor can
    therefore never take over the same run while this attempt is still touching
    the database, and the token fence discards anything this attempt publishes
    late.
    """
    with engine.connect() as guard:
        key = execution_key(run_id)
        acquired = guard.execute(text('SELECT pg_try_advisory_lock(CAST(:key AS bigint))'), {'key': key}).scalar_one()
        # The lock is session-scoped; ending the implicit transaction right away
        # keeps the guard connection out of idle-in-transaction for the whole run.
        guard.commit()
        if not acquired:
            # The previous attempt is still inside its database phase. Its claim
            # was recovered early, so hand the run back and let it be re-admitted
            # once the old attempt has really released the lock.
            logging.warning('Execution lock busy for run %s; requeueing', run_id)
            requeue(run_id, token)
            return
        try:
            try:
                result = solve(payload, engine)
                finish(run_id, token, result=result)
            except (ValueError, KeyError, TypeError) as exc:
                finish(run_id, token, error=str(exc)[:500])
            except Exception:
                logging.exception('Domain run failed: %s', run_id)
                finish(run_id, token, error='后台执行失败，请检查输入或查看 worker 日志。')
        finally:
            # Always drop the session lock so a lost connection cannot pin the
            # run's execution key after this attempt is gone.
            try:
                guard.execute(text('SELECT pg_advisory_unlock(CAST(:key AS bigint))'), {'key': key})
                guard.commit()
            except Exception:
                logging.exception('Failed to release execution lock for run %s', run_id)


def slot(stop):
    while not stop.is_set():
        try:
            job = claim()
            HEARTBEAT.touch()
            if not job:
                # A queued batch may be addable on a disjoint month even while
                # this slot just saw a collision; only sleep when nothing is
                # queued, so free slots keep draining parallel work.
                if has_ready():
                    sleep(0.2)
                else:
                    cache.brpop('worker:wake', timeout=1)
                continue
            execute(*job)
        except Exception:
            logging.exception('Worker loop failure')
            sleep(2)


def work():
    HEARTBEAT.unlink(missing_ok=True)
    stop = Event()
    beat = Thread(target=heartbeat, args=(stop,), daemon=True)
    beat.start()
    try:
        threads = [Thread(target=slot, args=(stop,), name=f'slot-{index}', daemon=True) for index in range(SLOTS)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    finally:
        stop.set()
        beat.join()


if __name__ == '__main__':
    work()
