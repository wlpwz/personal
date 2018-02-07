import sys
import traceback
import threading

_lock = threading.RLock()
_threads = []
_stopped = False

class Thread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.started = False
        self.interrupt_flag = False
        self._sleep_cond = threading.Condition()

    def acquire_lock(self):
        self._sleep_cond.acquire()

    def release_lock(self):
        self._sleep_cond.release()

    def interrupt(self):
        if not self.interrupt_flag:
            self.interrupt_flag = True
            self.wakeup()
            _lock.acquire()
            try:
                _threads.remove(self)
            finally:
                _lock.release()

    def start(self):
        _lock.acquire()
        try:
            if _stopped:
                raise Exception('Already stopped')
            _threads.append(self)
        finally:
            _lock.release()
        threading.Thread.start(self)
        self.started = True

    def sleep(self, timeout=None):
        self._sleep_cond.acquire()
        try:
            if not self.interrupt_flag:
                self._sleep_cond.wait(timeout)
        except IOError:
            print sys.exc_info()
            if sys.exc_info()[1] == 514:
                pass # Python bug, ignore it
            else:
                raise
        finally:
            self._sleep_cond.release()

    def wakeup(self):
        self._sleep_cond.acquire()
        self._sleep_cond.notifyAll()
        self._sleep_cond.release()

def stop_all():
    _lock.acquire()
    _stopped = True
    try:
        for thread in _threads * 1:
            thread.interrupt()
    finally:
        _lock.release()
