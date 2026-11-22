"""Unit tests for the month-scoped scheduling core.

Run from ``backend/``:  python -m unittest discover -s tests
These cover the pure decision logic; the database wiring (advisory locks, token
fencing) is exercised by the worker at runtime.
"""
import unittest

from src.services import schedule


C = schedule.Candidate


class DecideTests(unittest.TestCase):
    def decide(self, *cands):
        return schedule.decide(list(cands))

    def test_first_batch_runs_and_colliding_batch_waits(self):
        decision = self.decide(
            C(1, 'queued', ('2026-07', '2026-08', '2026-09', '2026-10', '2026-11')),
            C(2, 'queued', ('2026-08', '2026-12')))
        self.assertEqual(decision.granted, 1)
        self.assertEqual(decision.states[2], schedule.WAITING)
        self.assertEqual(decision.waiting[2], ('2026-08',))

    def test_disjoint_batch_bypasses_waiting_batch(self):
        decision = self.decide(
            C(1, 'running', ('2026-08',)),
            C(2, 'waiting', ('2026-08', '2026-09')),
            C(3, 'queued', ('2026-10',)))
        self.assertEqual(decision.granted, 3)
        self.assertEqual(decision.states[2], schedule.WAITING)

    def test_later_batch_cannot_overtake_colliding_waiting_batch(self):
        decision = self.decide(
            C(1, 'running', ('2026-08',)),
            C(2, 'queued', ('2026-09',)),
            C(3, 'queued', ('2026-09',)))
        self.assertEqual(decision.granted, 2)
        self.assertEqual(decision.states[3], schedule.WAITING)
        self.assertEqual(decision.waiting[3], ('2026-09',))

    def test_waiting_batch_holds_no_other_month(self):
        # The earlier batch is blocked on 09 but still reserves 10 for FIFO, so
        # the later batch cannot jump it there.
        decision = self.decide(
            C(1, 'running', ('2026-09',)),
            C(2, 'queued', ('2026-09', '2026-10')),
            C(3, 'queued', ('2026-10',)))
        self.assertEqual(decision.granted, None)
        self.assertEqual(decision.states[2], schedule.WAITING)
        self.assertEqual(decision.states[3], schedule.WAITING)

    def test_recovery_respects_old_execution_lock(self):
        # Expired lease but the old execution still holds the lock: do not take over.
        decision = self.decide(
            C(1, 'running', ('2026-08',), lease_alive=False, lock_free=False),
            C(2, 'queued', ('2026-08',)))
        self.assertEqual(decision.recovered, ())
        self.assertEqual(decision.granted, None)
        self.assertEqual(decision.states[2], schedule.WAITING)

    def test_recovery_restores_original_position(self):
        # Expired lease and free lock: requeue at its original position, so the
        # successor still waits behind it.
        decision = self.decide(
            C(1, 'running', ('2026-08',), lease_alive=False, lock_free=True),
            C(2, 'queued', ('2026-08',)))
        self.assertEqual(decision.recovered, (1,))
        self.assertEqual(decision.granted, 1)
        self.assertEqual(decision.states[2], schedule.WAITING)

    def test_live_run_holds_its_months(self):
        decision = self.decide(
            C(1, 'running', ('2026-08',)),
            C(2, 'queued', ('2026-08',)))
        self.assertEqual(decision.granted, None)
        self.assertEqual(decision.states[2], schedule.WAITING)

    def test_one_admission_per_pass(self):
        decision = self.decide(
            C(1, 'queued', ('2026-08',)),
            C(2, 'queued', ('2026-10',)),
            C(3, 'queued', ('2026-08',)))
        self.assertEqual(decision.granted, 1)
        self.assertEqual(decision.states[2], schedule.QUEUED)
        self.assertEqual(decision.states[3], schedule.WAITING)

    def test_empty_month_batch_is_not_blocked(self):
        decision = self.decide(C(9, 'queued', ()))
        self.assertEqual(decision.granted, 9)

    def test_waiting_reservation_chains_transitively(self):
        # A waits behind the live run on 08; B waits behind A on 08; C only
        # touches 09 but still waits behind B, which reserved 09 while waiting.
        decision = self.decide(
            C(1, 'running', ('2026-08',)),
            C(2, 'queued', ('2026-08',)),
            C(3, 'queued', ('2026-08', '2026-09')),
            C(4, 'queued', ('2026-09',)))
        self.assertEqual(decision.granted, None)
        self.assertEqual(decision.waiting[2], ('2026-08',))
        self.assertEqual(decision.waiting[3], ('2026-08',))
        self.assertEqual(decision.waiting[4], ('2026-09',))

    def test_chain_advances_after_blocker_clears(self):
        decision = self.decide(
            C(2, 'queued', ('2026-08',)),
            C(3, 'queued', ('2026-08', '2026-09')),
            C(4, 'queued', ('2026-09',)))
        self.assertEqual(decision.granted, 2)
        self.assertEqual(decision.states[3], schedule.WAITING)
        self.assertEqual(decision.states[4], schedule.WAITING)


if __name__ == '__main__':
    unittest.main()
