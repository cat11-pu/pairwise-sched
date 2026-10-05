# pairwise-sched

一个只用 Python 标准库实现的多级反馈队列（MLFQ）协程调度器实验项目。
不用真实线程：任务由生成器函数描述，时钟可注入，行为完全确定。

## 用途

`sched/core.py` 是一个模拟调度内核，负责：

- 多级反馈队列：层级 0 优先级最高，末级最低；
- 时间片：同一级队列内按时间片轮转，用完时间片降到下一级；
- 优先级老化：排队等待过久的任务会被提升回更高层级，避免饿死；
- 阻塞与唤醒：任务可以主动阻塞若干拍，到点自动唤醒，也支持外部事件唤醒；
- 统计：忙拍数、空转拍数、等待拍数、周转时间、切换次数等。

任务体是一个生成器函数，每次产出一个请求：

- `CPU`（模块里的单例）表示申请一拍 CPU；
- `Block(n)` 表示阻塞 n 拍后自动唤醒；
- 生成器结束表示任务完成。

## 目录

```
sched/__init__.py     对外导出
sched/core.py         调度器内核
tests/test_core.py    验收测试
```

## 怎么跑测试

在项目根目录执行：

```
python3 -m unittest discover -s tests -v
```

Windows 上用任意 Python 3 解释器执行同一条命令即可，例如：

```
C:/Users/Administrator/AppData/Local/Programs/Python/Python313/python.exe -m unittest discover -s tests -v
```

## 最小示例

```python
from sched import CPU, Block, Scheduler, VirtualClock


def worker(rounds):
    for _ in range(rounds):
        yield CPU
        yield Block(2)


clock = VirtualClock()
sched = Scheduler(clock, quantum=2, aging=6)
sched.add("worker", worker(3))
stats = sched.run()
print(stats)
```

`sched.history` 按时间顺序记录了每一次任务请求（时间、任务名、层级、本片已用拍数、请求对象），
`sched.queues_snapshot()` 与 `sched.locate(name)` 可以查看任务当前排在哪几个队列里。
