"""
Concurrency with coroutine, thread and process.
"""

import asyncio
import threading
import random
import logging
from multiprocessing import Process, Pipe, Queue


class Task(object):
    def __init__(self, pid, tid):
        self.pid = pid
        self.tid = tid

    def __repr__(self) -> str:
        return 'pid:{:d} tid:{}'.format(self.pid, self.tid)

    async def run(self):
        pass


class TaskStatus(object):
    def __init__(self, pid, tid, succeeded=True, reason=None):
        self.pid = pid
        self.tid = tid
        self.succeeded = succeeded
        self.reason = reason if reason is not None else 'finished'

    def __repr__(self) -> str:
        return 'pid:{:d} tid:{} succeeded:{:s} for reason {:s}' \
            .format(self.pid, self.tid, str(self.succeeded), self.reason)

    def is_succeeded(self):
        return self.succeeded


class TaskHandler(object):
    def __init__(self, pipe: Pipe, queue: Queue, name: str = 'TaskHandler'):
        self.pipe = pipe
        self.queue = queue
        self.loop = None
        self.logger = logging.getLogger(name)

    def on_task_result(self, result):
        pass

    def start(self):
        self.loop = asyncio.get_event_loop()
        threading.Thread(target=self._listen_pipe).start()
        self.loop.run_forever()

    def _listen_pipe(self):
        while True:
            task = self.pipe.recv()
            asyncio.run_coroutine_threadsafe(self._handle_task(task), self.loop)

    async def _handle_task(self, task: Task):
        succeeded = True
        reason = None
        try:
            result = await task.run()
            self.on_task_result(result)
        except Exception as e:
            self.logger.error('Task {:s} failed for {:s}'.format(repr(task), repr(e)))
            succeeded = False
            reason = repr(e)
        self.queue.put(TaskStatus(task.pid, task.tid, succeeded, reason))


class JobMonitor(object):
    def on_process_creation(self, pid):
        pass

    def on_task_creation(self, task: Task):
        pass

    def on_task_stopping(self, status: TaskStatus):
        pass


class JobScheduler(object):
    def __init__(self, pcount: int, monitor: JobMonitor = None):

        self.pcount = pcount
        self.pipes = {}
        self.queue = Queue()
        self.monitor = monitor

    def start(self, handler_cls=TaskHandler, block=False):
        for i in range(self.pcount):
            pipe, child_pipe = Pipe()
            p = Process(target=handler_cls(child_pipe, self.queue).start)
            p.start()

            if self.monitor is not None:
                self.monitor.on_process_creation(p.pid)

            self.pipes[p.pid] = pipe

        if block:
            self._listen_queue()
        else:
            threading.Thread(target=self._listen_queue).start()

    def add_task(self, task: Task):
        pid = random.choice(list(self.pipes.keys())) if task.pid is None else task.pid

        self.pipes[pid].send(task)

        if self.monitor is not None:
            self.monitor.on_task_creation(task)

    def _listen_queue(self):
        while True:
            status = self.queue.get()
            if self.monitor is not None:
                self.monitor.on_task_stopping(status)
