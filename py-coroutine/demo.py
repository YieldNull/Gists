import logging
import random
import time
import asyncio
from cwctp import JobMonitor, Task, TaskStatus, Scheduler


class DemoMonitor(JobMonitor):
    def __init__(self):
        self.tasks = {}
        self.logger = logging.getLogger('JobMonitor')

    def on_process_creation(self, pid):
        self.tasks[pid] = {
            'running': set(),
            'finished': set()
        }
        self.logger.info('Starting process {:d}'.format(pid))

    def on_task_creation(self, task: Task):
        p = self.tasks[task.pid]

        p['running'].add(task.tid)

        self.logger.info('Scheduled task: {:s}'.format(repr(task)))

    def on_task_stopping(self, status: TaskStatus):
        p = self.tasks[status.pid]

        p['running'].remove(status.tid)
        p['finished'].add(status.tid)

        self.logger.info('Task finished: {:s}'.format(repr(status)))


class DemoTask(Task):
    def __init__(self, pid, tid):
        super().__init__(pid, tid)

    async def run(self):
        await asyncio.sleep(1)


if __name__ == '__main__':
    import sys

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    monitor = DemoMonitor()
    scheduler = Scheduler(4, monitor=monitor)
    scheduler.start()

    tid = 0
    while True:
        for i in range(random.randint(0, 10)):
            pid, _ = min(monitor.tasks.items(), key=lambda x: len(x[1]['running']))
            scheduler.add_task(DemoTask(pid, tid))
            tid += 1

        time.sleep(2)
