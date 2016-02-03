import logging
from gevent import Greenlet
from gevent.event import AsyncResult

__author__ = "Piotr Gawlowicz, Mikolaj Chwalisz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "{gawlowicz, chwalisz}@tkn.tu-berlin.de"

def _clean_after_use(fn):
    def wrapped(self, *args, **kwargs):
        if self._ctrl._blocking:
            self._ctrl._asyncResults["blocking"] = AsyncResult()

        fn(self, *args, **kwargs)
        self._ctrl._scope = None
        self._ctrl._exec_time = None
        self._ctrl._delay = None
        self._ctrl._timeout = None
        self._ctrl._callback = None

        if self._ctrl._blocking:
            self._ctrl._blocking = False
            response = self._ctrl._asyncResults["blocking"].get()
            del self._ctrl._asyncResults["blocking"] 
            return response

    return wrapped

class UPIRadio(object):
    def __init__(self, ctrl):
        self._ctrl = ctrl
        self._msg_type = "RADIO"

    @_clean_after_use
    def set_channel(self, ch):
        '''
        set_channel 
        ch - channel to set
        '''
        #pass
        cmd = "set_channel"
        self._ctrl.send(None, cmd, msg_type=self._msg_type)

    @_clean_after_use
    def set_power(self, pwr=None):
        return 0

    @_clean_after_use
    def start_server(self):
        '''
        func desc
        '''
        msg = "start_server"
        self._ctrl.send(None, msg, msg_type=self._msg_type)