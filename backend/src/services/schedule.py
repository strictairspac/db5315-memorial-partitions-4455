"""Month-scoped admission control for concurrent retention replays.

A batch touches a set of months (from its partition plan and the entry dates of
its slips). Two batches that share a month must never touch the database at the
same time, while disjoint batches may run in parallel.

The decision logic is kept pure so it can be reasoned about and tested without a
database. ``src.services.workflow`` feeds it a snapshot of the run table and
applies the result inside one serialized transaction.

Rules implemented here:

* all-or-nothing: a batch is either granted every month it needs or holds none
  while it waits, so a blocked month never occupies the others;
* first-in-first-out per month: a later batch may bypass a waiting batch that it
  has no month in common with, but never overtake one it collides with;
* recovery: a running attempt whose lease expired and whose execution lock is
  free is put back at its original queue position before the grant pass.
"""

from dataclasses import dataclass, field

RUNNING = 'running'
QUEUED = 'queued'
WAITING = 'waiting'

# Advisory lock keys (signed 64-bit). The scheduler key serialises claim passes;
# each run pins its own execution key while it is allowed to touch the database.
SCHEDULER_KEY = -5_315_000
EXECUTION_KEY_BASE = 7_531_500_000


@dataclass(frozen=True)
class Candidate:
    id: int
    status: str
    months: tuple[str, ...]
    lease_alive: bool = True
    lock_free: bool = False  # only consulted for an expired running attempt


@dataclass(frozen=True)
class Decision:
    recovered: tuple[int, ...]
    granted: int | None
    states: dict[int, str]
    waiting: dict[int, tuple[str, ...]] = field(default_factory=dict)


def execution_key(run_id):
    return EXECUTION_KEY_BASE + run_id


def decide(candidates):
    """Return the recovery, grant and waiting decisions for one scheduler pass."""
    items = sorted(candidates, key=lambda item: item.id)
    states = {}
    recovered = []
    for item in items:
        if item.status == RUNNING and not item.lease_alive and item.lock_free:
            recovered.append(item.id)
            states[item.id] = QUEUED
        elif item.status == RUNNING:
            states[item.id] = RUNNING
        else:
            states[item.id] = QUEUED

    # Month -> ids that already hold it: live runs plus every earlier pending
    # batch that wants it. Waiting batches are not added, so they hold nothing.
    held = {}
    for item in items:
        if states[item.id] == RUNNING:
            for month in item.months:
                held.setdefault(month, set()).add(item.id)

    earlier = {}
    waiting = {}
    granted = None
    for item in items:
        if states[item.id] != QUEUED:
            continue
        months = set(item.months)
        blockers = sorted(month for month in months if held.get(month) or month in earlier)
        for month in months:
            earlier.setdefault(month, item.id)
        if blockers:
            states[item.id] = WAITING
            waiting[item.id] = tuple(blockers)
        elif granted is None:
            granted = item.id
            states[item.id] = RUNNING
            for month in months:
                held.setdefault(month, set()).add(item.id)
        else:
            # Only one admission per pass; remaining eligible batches stay
            # queued (or report the months the newly granted batch now holds).
            held_blockers = sorted(month for month in months if held.get(month))
            states[item.id] = WAITING if held_blockers else QUEUED
            waiting[item.id] = tuple(held_blockers)

    for item in items:
        waiting.setdefault(item.id, ())
    return Decision(tuple(recovered), granted, states, waiting)
