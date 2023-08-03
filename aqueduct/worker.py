import multiprocessing as mp
import queue
import sys
from concurrent.futures import ThreadPoolExecutor, Future, wait, FIRST_COMPLETED
from threading import BrokenBarrierError
from typing import Dict, Iterator, List, Optional

from .handler import BaseTaskHandler
from .logger import log
from .metrics.timer import (
    Timer,
    timeit,
)
from .queues import FlowStepQueue, select_next_queue
from .task import BaseTask, DEFAULT_PRIORITY, StopTask

MAX_SPIN_ITERATIONS=1000


class Worker:
    """Обертка над классом BaseTaskHandler.

    Достает из входной очереди задачу, пропускает ее через обработчика task_handler и кладет ее
    в выходную очередь.
    """

    def __init__(
            self,
            queues: List[FlowStepQueue],
            task_handler: BaseTaskHandler,
            batch_size: int,
            batch_timeout: float,
            step_number: int,
    ):
        self._queues = queues
        self._queue_priorities = len(self._queues)
        self.task_handler = task_handler
        self.name = task_handler.__class__.__name__
        self.step_name = self.task_handler.get_step_name(step_number)
        self._batch_size = batch_size
        self._batch_timeout = batch_timeout
        self._stop_task: BaseTask = None  # noqa
        self._step_number = step_number

    def _start(self):
        """Runs something huge (e.g. model) in child process."""
        self.task_handler.on_start()

    def _wait_task(self, timeout: float, queue_in: mp.Queue) -> Optional[BaseTask]:
        task = None
        # There is a problem with Python MP Queue implementation.
        # queue.get() can raise Empty exception even thou queue is in fact not empty.
        # For example, if we have 10 tasks in a queue, we expect batch size to be 10, but it is not always true, because
        #  after reading, say, 5 tasks, Python queue can tell as that there is nothing left, which is in fact false.
        # In our implementation we use an additional spin over a queue to guarantee consistent results.
        # More info: https://bugs.python.org/issue23582
        i = 0
        while not task and i < MAX_SPIN_ITERATIONS:
            # Limit our spin with maximum of MAX_SPIN_ITERATIONS just in case.
            # We do not want long, purposeless iteration
            i += 1
            try:
                if timeout == 0:
                    task = queue_in.get(block=False)
                else:
                    task = queue_in.get(timeout=timeout)
            except queue.Empty:
                # additionally check queue size to make sure that there is in fact no tasks there.
                # if size > 0 then Empty exception was fake -> we should try to call get() again
                # since macos does not support qsize method - ignore that check
                # (it would be great to make a proper queue later)
                if sys.platform == 'darwin' or queue_in.qsize() == 0:
                    break

        if not task:
            return

        if isinstance(task, StopTask):
            self._stop_task = task
            return

        log.debug(f'[{self.name}] Have message')

        task.metrics.stop_transfer_timer(self.step_name)
        task_size = getattr(queue_in, 'task_size', None)
        if task_size:
            task.metrics.save_task_size(task_size, self.step_name)

        # don't pass an expired task to the next steps
        if task.is_expired():
            log.debug(f'[{self.name}] Task expired. Skip: {task}')
            return

        return task

    def _get_batch_with_timeout(self, batch: List[BaseTask], queue_in: mp.Queue) -> List[BaseTask]:
        """ Collecting incoming tasks into batch.
        We will be waiting until number of tasks in batch would be equal `batch_size`.
        If batch could not be fully collected in batch_timeout time -> return what we have at that time.
        """
        timeout = self._batch_timeout
        while True:
            with timeit() as passed_time:
                task = self._wait_task(timeout, queue_in)

            if task:
                batch.append(task)

                if len(batch) == self._batch_size:
                    return batch

            timeout -= passed_time.seconds

            if self._stop_task or timeout <= 0:
                return batch

            # timeout should not be less then 1ms, to avoid unnecessary short sleeps
            timeout = max(timeout, 0.001)

    def _get_batch_dynamic(self, batch: List[BaseTask], queue_in: mp.Queue) -> List[BaseTask]:
        """ Collecting incoming tasks into batch.
        This method will not be waiting for batch_timeout to collect full batch,
        we just simply get all tasks that are currently in the queue and making batch only from them.
        """
        while True:
            task = None
            task = self._wait_task(timeout=0.0, queue_in=queue_in)

            if task:
                batch.append(task)
                if len(batch) == self._batch_size:
                    break
            else:
                break

        return batch

    def _wait_batch(self, step_queue: FlowStepQueue) -> List[BaseTask]:
        batch = []
        timer = Timer()

        # wait for the first task
        while True:
            # wait for some long amount of time (10 secs), and wait again,
            # until we eventually get a task
            task = self._wait_task(10.0, step_queue.queue)
            if task:
                batch.append(task)
                timer.start()
                break
            elif self._stop_task:
                return []

        # waiting for the rest of the batch if batch_size > 1
        if self._batch_size > 1:
            if self._batch_timeout > 0:
                batch = self._get_batch_with_timeout(batch, step_queue.queue)
            else:
                batch = self._get_batch_dynamic(batch, step_queue.queue)

        timer.stop()
        if self._batch_size > 1:
            batch[0].metrics.batch_times.add(self.step_name, timer.seconds)
            batch[0].metrics.batch_sizes.add(self.step_name, len(batch))

        return batch

    def _wait_batch_with_lock(self, step_queue: FlowStepQueue) -> List[BaseTask]:
        if step_queue.batch_lock is not None:
            with step_queue.batch_lock:
                return self._wait_batch(step_queue=step_queue)
        else:
            return self._wait_batch(step_queue=step_queue)

    def _iter_batches(self) -> Iterator[Optional[List[BaseTask]]]:
        """ Returns iterator over input task batches.
        If there is no tasks in queue -> block until task appears.
        This iterator takes into account batch_timeout and batch size.
        If input task is expired or filtered by condition - it would not be placed in batch.
        """
        running_tasks: Dict[int, Future] = {}
        with ThreadPoolExecutor(max_workers=self._queue_priorities) as executor:
            while True:
                # process queues with maximum priority first
                for priority in reversed(range(self._queue_priorities)):
                    if running_tasks.get(priority) is None:
                        task = executor.submit(
                            self._wait_batch_with_lock,
                            self._queues[priority][self._step_number - 1]
                        )
                        running_tasks[priority] = task
                wait(running_tasks.values(), return_when=FIRST_COMPLETED)

                for priority in reversed(range(self._queue_priorities)):
                    task = running_tasks.get(priority)
                    if task is not None and task.done():
                        running_tasks[priority] = None
                        batch = task.result()
                        if batch:
                            yield batch

                if self._stop_task:
                    break

    def _post_handle(self, task: BaseTask):
        task.metrics.start_transfer_timer(self.step_name)
        queue_out = select_next_queue(
            queues=self._queues,
            task=task,
            start_index=self._step_number,
        )
        queue_out.put(task)

    def loop(self, pid: int, start_barrier: mp.Barrier):
        """Main worker loop.

        The code below is executed in a new process.
        """
        log.info(f'[Worker] initialising handler {self.name}')
        self._start()
        log.info(f'[Worker] handler {self.name} ok, waiting for others to start')

        try:
            start_barrier.wait()
        except BrokenBarrierError:
            raise TimeoutError('Starting timeout expired')

        log.info(f'[Worker] handler {self.name} ok, starting loop')

        for tasks_batch in self._iter_batches():
            with timeit() as timer:
                self.task_handler.handle(*tasks_batch)

            for task in tasks_batch:
                task.metrics.handle_times.add(self.step_name, timer.seconds)
                self._post_handle(task)

        if self._stop_task:
            self._queues[DEFAULT_PRIORITY][self._step_number].queue.put(self._stop_task)
