"""
This file serves as an example to use this decorator
It is also a unit test file. Feel free to add more to benchmark it.
"""
import unittest
try:
    from iodeco import *
except SystemError or ImportError:
    from .iodeco import *
from threading import Thread
import cv2
import numpy as np
import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
from time import perf_counter


class DecoratorTests(unittest.TestCase):
    def test_video(self):
        # Note: Threading is not guaranteed a performance increase for every IO problem
        # Note: It really depends on how long your processing time is relative to your IO time
        # Note: a too-small or too-large max_len will also lead to a performance or memory issue respectively
        logging.debug("Testing Video...")
        vidcap = cv2.VideoCapture("../videos/orig.mp4")
        vidwriter = cv2.VideoWriter("../videos/test_result.mp4", cv2.VideoWriter_fourcc(*'mp4v'),
                                    vidcap.get(cv2.CAP_PROP_FPS), (1280, 720))
        ovidwriter = cv2.VideoWriter("../videos/gt_result.mp4", cv2.VideoWriter_fourcc(*'mp4v'),
                                     vidcap.get(cv2.CAP_PROP_FPS), (1280, 720))
        max_len = 10

        # HERE is how you can use the decorator
        # create session first
        # then pass it to your function decorator
        iohandler = create_session(max_len)
        test_result_frames = []

        @rio(iohandler)
        def load():
            _batch_frames = np.zeros((150, 720, 1280, 3), dtype=np.uint8)
            _frame_pointer = 0
            _nomore = False
            for i in range(150):
                _ret, _fr = vidcap.read()
                if _ret:
                    _batch_frames[i, ...] = _fr
                    _frame_pointer += 1
                else:
                    _nomore = True
                    break
            return _batch_frames[:_frame_pointer], _nomore

        @wio(iohandler)
        def write(data2write):
            # because iohandler doesn't check payload validity, you will need to do this in your own write function
            if data2write is None: return
            for i in range(len(data2write)):
                vidwriter.write(data2write[i])
                test_result_frames.append(data2write[i])

        def img_prop(_f):
            _f = cv2.bilateralFilter(_f, 9, 75, 75)
            # _f = cv2.bilateralFilter(_f, 9, 75, 75)
            # _f = cv2.bilateralFilter(_f, 9, 75, 75)
            # _f = cv2.bilateralFilter(_f, 9, 75, 75)
            # _f = cv2.bilateralFilter(_f, 9, 75, 75)
            # _f = cv2.bilateralFilter(_f, 9, 75, 75)
            # _f = cv2.bilateralFilter(_f, 9, 75, 75)

            return _f

        @pio(iohandler)
        def process(payload):
            if payload[0].shape[0] > 0:
                result_frame = []
                for i in range(payload[0].shape[0]):
                    result_frame.append(img_prop(payload[0][i, ...]))
                return result_frame
            else:
                return None

        # TODO make this kick off itself
        # Right now you'll need to kick off this yourself
        s = perf_counter()
        threads = list()
        threads.append(Thread(target=load))
        threads.append(Thread(target=write))
        threads.append(Thread(target=process))
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        logging.debug("Thread takes %.2f FPS" % (len(test_result_frames) / (perf_counter() - s)))

        # No thread version - get ground truth frames
        frames = []
        vidcap.set(cv2.CAP_PROP_POS_FRAMES, 0)
        nomore = False
        s = perf_counter()
        while not nomore:
            batch_frames = np.zeros((150, 720, 1280, 3), dtype=np.uint8)
            frame_pointer = 0

            for ii in range(150):
                r, f = vidcap.read()
                if r:
                    batch_frames[ii, ...] = f
                    frame_pointer += 1
                else:
                    nomore = True
                    break

            for ii in range(frame_pointer):
                frames.append(img_prop(batch_frames[ii, ...]))
                ovidwriter.write(batch_frames[ii, ...])

        logging.debug("No thread takes %.2f FPS" % (len(frames) / (perf_counter() - s)))

        # test
        frames = np.array(frames)
        test_result_frames = np.array(test_result_frames)
        self.assertTrue(np.array_equal(frames, test_result_frames))


if __name__ == '__main__':
    unittest.main()
