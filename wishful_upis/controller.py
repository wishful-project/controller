import logging
import time
from gevent import (socket,
                    Greenlet,
                    )


class Controller(Greenlet):
    def __init__(self):
        Greenlet.__init__(self)
        self.log = logging.getLogger("{module}.{name}".format(
            module=self.__class__.__module__, name=self.__class__.__name__))

    nodes = None
    groups = None
    msg_type = {} # 'full name': [(group, callback)]

    def _run(self):
        while True:
            time.sleep(1)

    def add_callback(self, group, callback):
        def test(group):
            pass
        # assert callable(callback)
        self.log.debug("added callback {:s} for group {:s}".format(callback, group))
        return  test


class ConnectionManager(Greenlet):

    def __init__(self):
        Greenlet.__init__(self)
        self.log = logging.getLogger("{module}.{name}".format(
            module=self.__class__.__module__, name=self.__class__.__name__))
