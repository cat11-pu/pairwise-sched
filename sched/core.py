"""多级反馈队列（MLFQ）协程调度器内核。

任务用生成器函数描述：生成器每产出一个请求，调度器就按请求更新调度状态。
Cpu 请求消耗一拍 CPU，Block 请求把任务挂起若干拍后自动唤醒。
层级 0 是最高优先级队列，末级队列优先级最低。
调度器不依赖真实线程，虚拟时钟可注入，行为完全确定。
"""

from collections import deque

DEFAULT_QUANTUM = 2
DEFAULT_LEVELS = 3
DEFAULT_MAX_STEPS = 100000

READY = "ready"
RUNNING = "running"
BLOCKED = "blocked"
DONE = "done"


class Cpu:
    """请求：让任务消耗一拍 CPU。"""

    __slots__ = ()

    def __repr__(self):
        return "Cpu()"


CPU = Cpu()


class Block:
    """请求：让任务阻塞指定拍数后自动唤醒。"""

    __slots__ = ("ticks",)

    def __init__(self, ticks):
        if isinstance(ticks, bool) or not isinstance(ticks, int) or ticks < 1:
            raise ValueError("阻塞拍数必须是正整数")
        self.ticks = ticks

    def __repr__(self):
        return "Block(%d)" % self.ticks


class VirtualClock:
    """可注入的虚拟时钟。"""

    def __init__(self, start=0):
        self._now = start

    def now(self):
        """返回当前虚拟时刻。"""
        return self._now

    def advance(self, ticks=1):
        """把虚拟时钟向前推进。"""
        if ticks < 0:
            raise ValueError("虚拟时钟不能倒退")
        self._now += ticks


class Task:
    """被调度的协程任务。"""

    def __init__(self, name, coro):
        if not isinstance(name, str) or not name:
            raise ValueError("任务名必须是非空字符串")
        if not hasattr(coro, "__next__"):
            raise TypeError("任务体必须是生成器")
        self.name = name
        self.coro = coro
        self.level = 0
        self.state = READY
        self.used = 0
        self.executed = 0
        self.slices = 0
        self.waited = 0
        self.wait_ticks = 0
        self.wake_at = None
        self.created_at = 0
        self.finished_at = None

    def __repr__(self):
        return "Task(%s)" % self.name


class Scheduler:
    """多级反馈队列调度器。"""

    def __init__(self, clock=None, quantum=DEFAULT_QUANTUM,
                 levels=DEFAULT_LEVELS, aging=None):
        if quantum < 1:
            raise ValueError("时间片必须大于 0")
        if levels < 1:
            raise ValueError("队列层数必须大于 0")
        if aging is not None and aging < 1:
            raise ValueError("老化间隔必须大于 0")
        self.clock = clock if clock is not None else VirtualClock()
        self.quantum = quantum
        self.aging = aging
        self.queues = [deque() for _ in range(levels)]
        self.blocked = []
        self.tasks = {}
        self.busy_ticks = 0
        self.idle_ticks = 0
        self.switches = 0
        self.history = []

    # ------------------------------------------------------------ 对外接口

    def add(self, name, coro, level=0):
        """注册任务并把它加入指定层级的就绪队列。"""
        if name in self.tasks:
            raise ValueError("任务名重复: %s" % name)
        if not 0 <= level < len(self.queues):
            raise ValueError("层级越界: %d" % level)
        task = Task(name, coro)
        task.level = level
        task.created_at = self.clock.now()
        self.tasks[name] = task
        self._enqueue_ready(task)
        return task

    def wake(self, name):
        """外部事件唤醒一个阻塞中的任务。"""
        task = self._task(name)
        if task.state is not BLOCKED:
            raise ValueError("任务 %s 不在阻塞状态" % name)
        task.state = READY
        task.level = max(0, task.level - 1)
        self._enqueue_ready(task)

    def step(self):
        """推进一步调度，返回本步执行的任务名；空闲时返回 None。"""
        if self.done():
            return None
        self._age_ready()
        self._release_sleepers()
        task = self._pick_next()
        if task is None:
            if self.blocked:
                self._advance_to_next_wake()
            return None
        self._run(task)
        return task.name

    def run(self, max_steps=DEFAULT_MAX_STEPS):
        """连续推进调度直到所有任务结束，返回统计结果。"""
        steps = 0
        while not self.done():
            if steps >= max_steps:
                raise RuntimeError("调度在 %d 步内没有收敛" % max_steps)
            self.step()
            steps += 1
        return self.stats()

    def done(self):
        """所有任务是否都已经结束。"""
        return all(task.state is DONE for task in self.tasks.values())

    def stats(self):
        """返回调度统计结果。"""
        finished = [t for t in self.tasks.values() if t.state is DONE]
        turnarounds = [t.finished_at - t.created_at for t in finished]
        return {
            "now": self.clock.now(),
            "busy": self.busy_ticks,
            "idle": self.idle_ticks,
            "executed": sum(t.executed for t in self.tasks.values()),
            "waited": sum(t.waited for t in self.tasks.values()),
            "switches": self.switches,
            "finished": len(finished),
            "avg_turnaround": (sum(turnarounds) / float(len(turnarounds))) if turnarounds else 0.0,
        }

    def queues_snapshot(self):
        """返回各就绪队列与阻塞列表中当前的任务名。"""
        snapshot = {}
        for level, queue in enumerate(self.queues):
            snapshot["level%d" % level] = [task.name for task in queue]
        snapshot["blocked"] = [task.name for task in self.blocked]
        return snapshot

    def locate(self, name):
        """返回任务当前所在的队列名列表。"""
        task = self._task(name)
        found = []
        for level, queue in enumerate(self.queues):
            if any(item is task for item in queue):
                found.append("level%d" % level)
        if any(item is task for item in self.blocked):
            found.append("blocked")
        return found

    def level_of(self, name):
        """返回任务当前所在层级。"""
        return self._task(name).level

    # ------------------------------------------------------------ 内部实现

    def _task(self, name):
        if name not in self.tasks:
            raise KeyError("未知任务: %s" % name)
        return self.tasks[name]

    def _enqueue_ready(self, task):
        """把任务加入其所在层级的就绪队列。"""
        self.queues[task.level].appendleft(task)

    def _pick_next(self):
        """按层级顺序取出下一个该运行的任务。"""
        for queue in self.queues:
            while queue:
                task = queue.popleft()
                if task.state is DONE:
                    continue
                return task
        return None

    def _run(self, task):
        """把 CPU 交给任务一次，并按它的请求更新调度状态。"""
        task.state = RUNNING
        self.switches += 1
        try:
            request = next(task.coro)
        except StopIteration:
            self._finish(task)
            return
        if isinstance(request, Cpu):
            self._charge_tick(task)
            if self._should_demote(task):
                self._demote(task)
            task.state = READY
            self._enqueue_ready(task)
            return
        if isinstance(request, Block):
            self._record(task, request)
            task.state = BLOCKED
            task.wake_at = self.clock.now() + request.ticks
            task.used = 0
            self.blocked.append(task)
            return
        task.state = DONE
        raise ValueError("任务 %s 产出了无法识别的请求: %r" % (task.name, request))

    def _charge_tick(self, task):
        """记一拍 CPU，并推进虚拟时钟。"""
        if task.used == 0:
            task.slices += 1
        task.used += 1
        task.executed += 1
        task.wait_ticks = 0
        self.busy_ticks += 1
        self.clock.advance(1)
        self._record(task, CPU)

    def _should_demote(self, task):
        """任务是否已经用完当前时间片。"""
        return task.used > self.quantum

    def _demote(self, task):
        """把用完时间片的任务降到下一级队列。"""
        if task.level < len(self.queues) - 1:
            task.level += 1

    def _finish(self, task):
        """结束任务。"""
        task.state = DONE
        task.finished_at = self.clock.now()

    def _record(self, task, request):
        """记录一次任务请求。"""
        self.history.append({
            "time": self.clock.now(),
            "task": task.name,
            "level": task.level,
            "used": task.used,
            "request": request,
        })

    def _release_sleepers(self):
        """把已经到唤醒时刻的阻塞任务放回就绪队列。"""
        now = self.clock.now()
        pending = []
        for task in self.blocked:
            if task.state is DONE:
                continue
            if task.wake_at is not None and task.wake_at <= now:
                task.wake_at = None
                task.state = READY
                self._enqueue_ready(task)
            else:
                pending.append(task)
        self.blocked = pending

    def _advance_to_next_wake(self):
        """空闲时把虚拟时钟推进到最近的唤醒时刻。"""
        targets = [t.wake_at for t in self.blocked
                   if t.wake_at is not None and t.state is not DONE]
        if not targets:
            return
        delta = min(targets) - self.clock.now()
        if delta <= 0:
            return
        self.clock.advance(delta)

    def _age_ready(self):
        """累计排队任务的等待拍数，并按需提升等待过久的任务。"""
        for level, queue in enumerate(self.queues):
            if not queue:
                continue
            for task in list(queue):
                task.waited += 1
                if self.aging is None or level == 0:
                    continue
                task.wait_ticks += 1
                if task.wait_ticks < self.aging:
                    continue
                queue.remove(task)
                task.level = level - 1
                self.queues[level - 1].append(task)
