"""sched.core 的验收测试。

只描述期望行为，覆盖正常流程、边界、异常输入、回归场景与调度不变量。
"""

import unittest

from sched import BLOCKED, CPU, DONE, READY, Block, Cpu, Scheduler, VirtualClock


def spin(ticks):
    """连续申请 ticks 拍 CPU。"""
    for _ in range(ticks):
        yield CPU


def spin_then_block(spin_ticks, block_ticks, tail_ticks):
    """先跑 spin_ticks 拍，再阻塞 block_ticks 拍，最后再跑 tail_ticks 拍。"""
    for _ in range(spin_ticks):
        yield CPU
    yield Block(block_ticks)
    for _ in range(tail_ticks):
        yield CPU


def cpu_ticks(sched):
    """取历史里真正消耗了 CPU 的那些记录。"""
    return [record for record in sched.history if isinstance(record["request"], Cpu)]


class SchedulerTests(unittest.TestCase):

    def test_single_task_runs_to_completion(self):
        clock = VirtualClock()
        sched = Scheduler(clock, quantum=2)
        sched.add("alpha", spin(5))

        sched.run()

        alpha = sched.tasks["alpha"]
        self.assertEqual([record["level"] for record in cpu_ticks(sched)], [0, 0, 1, 1, 2])
        self.assertEqual(alpha.executed, 5)
        self.assertEqual(alpha.state, DONE)
        self.assertEqual(clock.now(), 5)

    def test_time_slice_never_exceeds_quantum(self):
        clock = VirtualClock()
        sched = Scheduler(clock, quantum=3)
        sched.add("alpha", spin(8))

        sched.run()

        self.assertEqual([record["used"] for record in cpu_ticks(sched)],
                         [1, 2, 3, 1, 2, 3, 1, 2])
        self.assertEqual([record["level"] for record in cpu_ticks(sched)],
                         [0, 0, 0, 1, 1, 1, 2, 2])

    def test_same_level_tasks_rotate_fifo(self):
        clock = VirtualClock()
        sched = Scheduler(clock, quantum=100)
        sched.add("first", spin(3))
        sched.add("second", spin(3))

        sched.run()

        self.assertEqual([record["task"] for record in cpu_ticks(sched)],
                         ["first", "second", "first", "second", "first", "second"])
        self.assertEqual(sched.tasks["first"].executed, 3)
        self.assertEqual(sched.tasks["second"].executed, 3)

    def test_wakeup_promotes_to_higher_queue(self):
        clock = VirtualClock()
        sched = Scheduler(clock, quantum=1)
        sched.add("alpha", spin_then_block(1, 2, 2))

        sched.run()

        records = sched.history
        index = next(i for i, record in enumerate(records)
                     if isinstance(record["request"], Block))
        after = [record for record in records[index + 1:]
                 if isinstance(record["request"], Cpu)]
        self.assertTrue(after)
        self.assertEqual(after[0]["level"], max(0, records[index]["level"] - 1))
        self.assertEqual(sched.tasks["alpha"].executed, 3)

    def test_external_wake_keeps_single_queue_membership(self):
        clock = VirtualClock()
        sched = Scheduler(clock, quantum=10)
        sched.add("worker", spin_then_block(1, 6, 8))

        self.assertRaises(ValueError, Block, 0)
        self.assertRaises(ValueError, sched.add, "worker", spin(1))
        self.assertRaises(KeyError, sched.wake, "ghost")

        sched.step()
        sched.step()
        self.assertEqual(sched.tasks["worker"].state, BLOCKED)

        sched.wake("worker")
        self.assertEqual(sched.tasks["worker"].state, READY)
        self.assertEqual(sched.locate("worker"), ["level0"])

        doubled = []
        for _ in range(60):
            if sched.done():
                break
            sched.step()
            if len(sched.locate("worker")) > 1:
                doubled.append(clock.now())

        self.assertEqual(doubled, [])
        self.assertEqual(sched.tasks["worker"].executed, 9)

    def test_aging_promotes_after_wait_interval(self):
        clock = VirtualClock()
        sched = Scheduler(clock, quantum=10, aging=3)
        sched.add("hog", spin(30))
        sched.add("starved", spin(2), level=2)

        for _ in range(5):
            sched.step()
        self.assertEqual(sched.level_of("starved"), 1)

        sched.step()
        self.assertEqual(sched.level_of("starved"), 0)

        sched.run(max_steps=200)
        self.assertEqual(sched.tasks["starved"].executed, 2)

    def test_idle_clock_matches_executed_work(self):
        clock = VirtualClock()
        sched = Scheduler(clock, quantum=10)
        sched.add("sleeper", spin_then_block(1, 5, 1))

        started = clock.now()
        sched.run()

        stats = sched.stats()
        self.assertEqual(sched.idle_ticks, 5)
        self.assertEqual(stats["busy"], 2)
        self.assertEqual(stats["executed"], 2)
        self.assertEqual(clock.now() - started, stats["busy"] + stats["idle"])

    def test_waiting_time_excludes_own_execution(self):
        clock = VirtualClock()
        sched = Scheduler(clock, quantum=2)
        sched.add("solo", spin(4))

        sched.run()

        solo = sched.tasks["solo"]
        self.assertEqual(solo.waited, 0)
        self.assertEqual(sched.stats()["waited"], 0)
        self.assertEqual(solo.finished_at - solo.created_at, 4)

    def test_end_to_end_order_and_accounting(self):
        clock = VirtualClock()
        sched = Scheduler(clock, quantum=2)
        sched.add("alpha", spin(4))
        sched.add("beta", spin(4))
        sched.add("gamma", spin_then_block(1, 3, 1))

        sched.run()

        self.assertEqual([record["task"] for record in cpu_ticks(sched)],
                         ["alpha", "beta", "gamma", "alpha", "beta",
                          "alpha", "beta", "alpha", "gamma", "beta"])
        self.assertEqual(clock.now(), 10)
        self.assertEqual(sched.busy_ticks, sum(t.executed for t in sched.tasks.values()))
        self.assertEqual(sched.tasks["gamma"].executed, 2)


if __name__ == "__main__":
    unittest.main()
