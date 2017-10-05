import asyncio
import threading
import os
import time
import random

from multiprocessing import Process, Pipe, Queue


async def process_task(queue: Queue, tid):
    await asyncio.sleep(1)
    print('[Process-{:d}] Task finished. tid:{:d}'.format(os.getpid(), tid))
    queue.put((os.getpid(), tid))


def process_main(pipe: Pipe, queue: Queue):
    def listen_pipe(looop):
        while True:
            tid = pipe.recv()
            print('[Process-{:d}] receive task:{:d}'.format(os.getpid(), tid))

            asyncio.run_coroutine_threadsafe(process_task(queue, tid), looop)

    loop = asyncio.get_event_loop()

    threading.Thread(target=listen_pipe, args=(loop,)).start()

    loop.run_forever()


def schedule(pcount):
    pipes = {}
    tasks = {}

    queue = Queue()
    for i in range(pcount):
        pipe, child_pipe = Pipe()
        p = Process(target=process_main, args=(child_pipe, queue))
        p.start()

        pipes[p.pid] = pipe
        tasks[p.pid] = {
            'running': set(),
            'finished': set()
        }

    def listen_queue():
        while True:
            pid, tid = queue.get()
            print('[Scheduler] Task finished. pid:{:d} tid:{:d}'.format(pid, tid))

            p = tasks[pid]
            p['running'].remove(tid)
            p['finished'].add(tid)

    threading.Thread(target=listen_queue, args=()).start()

    tid = 0
    while True:
        for i in range(random.randint(0, 10)):
            pid, _ = min(tasks.items(), key=lambda x: len(x[1]['running']))

            tasks[pid]['running'].add(tid)

            pipes[pid].send(tid)

            print('[Scheduler] Schedule task:{:d} to process:{:d}'.format(tid, pid))

            tid += 1
        time.sleep(2)


if __name__ == '__main__':
    schedule(4)
