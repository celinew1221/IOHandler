from collections import deque
from threading import Thread, Lock, Condition


class IOHandler:
    def __init__(self, max_len=150):
        # Thread implementation of read, write and process
        self.readQueue = deque()
        self.writeQueue = deque()
        self.readDone = False
        self.max_len = max_len
        self.rlock, self.wlock, self.flag_lock, self.cv = Lock(), Lock(), Lock(), Condition()

    def set_max_len(self, max_len):
        self.max_len = max_len

    def read(self, load_fn):
        def wrapper():
            # last variable of your load_fn should return whether read is Done
            while True:
                payload = load_fn()
                self.rlock.acquire()
                self.readQueue.append(payload)
                # go to sleep if reach maximum limit
                with self.cv:
                    while len(self.readQueue) >= self.max_len:
                        self.rlock.release()
                        self.cv.wait()
                        self.rlock.acquire()
                self.rlock.release()

                # break when there's no more frames
                if payload[-1]: break

            # out of the loop, signal all process that it can terminate itself when complete all queue
            self.flag_lock.acquire()
            self.readDone = True
            self.flag_lock.release()
        return wrapper

    def has_more_to_write(self):
        # keepwriting when there's object in either of the Queue or reading is not Done
        self.wlock.acquire()
        self.rlock.acquire()
        self.flag_lock.acquire()
        keepgoing = not (self.readDone & len(self.writeQueue) == 0 & len(self.readQueue) == 0)
        self.wlock.release()
        self.rlock.release()
        self.flag_lock.release()
        return keepgoing

    def write(self, write_fn):
        def wrapper():
            keepwriting = True
            while keepwriting:
                # offload rendered object to write to tfrecords
                self.wlock.acquire()
                if len(writeQueue) == 0:
                    wlock.release()
                    keepwriting = self.has_more_to_write()
                    continue

                # has available payload
                payload = writeQueue.popleft()
                wlock.release()

                write_fn(payload)
                keepwriting = self.has_more_to_write()
        return wrapper

    def has_more_to_process(self):
        # update keepprocessing: when there's more to read or there's thing in readQueue to process
        self.rlock.acquire()
        self.flag_lock.acquire()
        keepgoing = not (self.readDone & len(self.readQueue) == 0)
        self.rlock.release()
        self.flag_lock.release()
        return keepgoing

    def process(self, process_func):
        def wrapper():
            keepprocessing = True
            while keepprocessing:
                # offload frames to render
                self.rlock.acquire()
                if len(self.readQueue) == 0:
                    self.rlock.release()
                    keepprocessing = self.has_more_to_process()
                    continue
                # has available payload
                payload = self.readQueue.popleft()

                # notify read agent if readQueue is available now
                with self.cv:
                    if len(self.readQueue) < self.max_len:
                        self.rlock.release()
                        self.cv.notify()

                # process
                data2write = process_func(payload)

                # write to writeQueue for IO
                if data2write is not None:
                    self.wlock.acquire()
                    self.writeQueue.append(data2write)
                    self.wlock.release()

                keepprocessing = self.has_more_to_process()
        return wrapper