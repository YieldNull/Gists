import logging
import random
import time
import asyncio
from cctp import Task, TaskStatus, TaskHandler, JobMonitor, JobScheduler


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
        return self.pid, self.tid, random.randint(0, 10)


class DemoHandler(TaskHandler):
    def on_task_result(self, result):
        self.logger.info('Task result ' + str(result))


if __name__ == '__main__':
    import sys

    logging.basicConfig(
        format='%(asctime)s [%(name)s] %(levelname)s pid-%(process)d : %(message)s',
        stream=sys.stdout,
        level=logging.INFO)

    monitor = DemoMonitor()
    scheduler = JobScheduler(4, monitor=monitor)
    scheduler.start(handler_cls=DemoHandler)

    tid = 0
    while True:
        for i in range(random.randint(0, 10)):
            pid, _ = min(monitor.tasks.items(), key=lambda x: len(x[1]['running']))
            scheduler.add_task(DemoTask(pid, tid))
            tid += 1

        time.sleep(2)
