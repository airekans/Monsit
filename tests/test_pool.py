import gevent.pool
import gevent.queue
import gevent.event
import time
import traceback


def timeit(func):
    def wrap(*args, **kwargs):
        begin_time = time.time()
        try:
            return func(*args, **kwargs)
        except:
            traceback.print_exc()
        finally:
            end_time = time.time()
            print 'function %s:' % func.__name__, end_time - begin_time
    return wrap

global_counter = 0


def test_func():
    global global_counter
    global_counter += 1
    return global_counter


class Pool(object):
    def __init__(self, pool_size=None):
        self._task_queue = gevent.queue.JoinableQueue()
        self._pool = gevent.pool.Pool(pool_size)
        if pool_size is None:
            pool_size = 100

        for _ in xrange(pool_size):
            self._pool.spawn(self.worker_func)

    def worker_func(self):
        while True:
            task = self._task_queue.get()
            if task is None:
                self._task_queue.task_done()
                break
            task()
            self._task_queue.task_done()

    def spawn(self, func, *args, **kwargs):
        task = lambda: func(*args, **kwargs)
        self._task_queue.put_nowait(task)

    def join(self):
        for _ in xrange(len(self._pool)):
            self._task_queue.put_nowait(None)
        self._task_queue.join()
        self._pool.join()

    def kill(self):
        self._pool.kill()


@timeit
def test_my_pool():
    pool = Pool(1000)
    for _ in xrange(100000):
        pool.spawn(test_func)

    pool.join()

@timeit
def test_gevent_pool():
    pool = gevent.pool.Pool(1000)
    for _ in xrange(100000):
        pool.spawn(test_func)

    pool.join()


if __name__ == '__main__':
    global_counter = 0
    test_gevent_pool()
    print 'global_counter', global_counter
    global_counter = 0
    test_my_pool()
    print 'global_counter', global_counter