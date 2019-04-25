from collections import deque
from threading import Lock, Condition
from time import sleep


class IOHandler:
    def __init__(self, max_len):
        # Thread implementation of rio, wio and pio
        self.readQueue = deque()
        self.writeQueue = deque()
        self.readDone = False
        self.max_len = max_len
        self.flag_lock, self.cv = Lock(), Condition()

    def set_max_len(self, max_len):
        self.max_len = max_len

    def read(self, load_fn):
        def wrapper():
            # last variable of your load_fn should return whether rio is Done
            while True:
                payload = load_fn()
                self.readQueue.append(payload)
                # break when there's no more frames
                if payload[-1]: break

                # go to sleep if reach maximum limit
                with self.cv:
                    while len(self.readQueue) >= self.max_len:
                        self.cv.wait()

            # out of the loop, signal all pio that it can terminate itself when complete all queue
            self.flag_lock.acquire()
            self.readDone = True
            self.flag_lock.release()
        return wrapper

    def has_more_to_write(self):
        # keepwriting when there's object in either of the Queue or reading is not Done
        self.flag_lock.acquire()
        keep_going = not (self.readDone and len(self.writeQueue) == 0 and len(self.readQueue) == 0)
        self.flag_lock.release()
        return keep_going

    def write(self, write_fn):
        def wrapper():
            keep_writing = True
            while keep_writing:
                # offload rendered object to wio to tfrecords
                if len(self.writeQueue) == 0:
                    keep_writing = self.has_more_to_write()
                    continue

                # has available payload
                payload = self.writeQueue.popleft()

                write_fn(payload)
                keep_writing = self.has_more_to_write()
        return wrapper

    def has_more_to_process(self):
        # update keep_processing: when there's more to rio or there's thing in readQueue to pio
        self.flag_lock.acquire()
        keep_going = not (self.readDone and len(self.readQueue) == 0)
        self.flag_lock.release()
        return keep_going

    def process(self, process_func):
        def wrapper():
            keep_processing = True
            while keep_processing:
                # offload frames to render
                if len(self.readQueue) == 0:
                    keep_processing = self.has_more_to_process()
                    continue
                # has available payload
                payload = self.readQueue.popleft()

                # notify rio agent if readQueue is available now
                with self.cv:
                    if len(self.readQueue) < self.max_len:
                        self.cv.notify()

                data2write = process_func(payload)

                # wio to writeQueue for IO
                if data2write is not None:
                    self.writeQueue.append(data2write)

                keep_processing = self.has_more_to_process()

        return wrapper
