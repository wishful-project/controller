import logging
from gevent import Greenlet
from gevent.event import AsyncResult

__author__ = "Piotr Gawlowicz, Mikolaj Chwalisz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "{gawlowicz, chwalisz}@tkn.tu-berlin.de"

def _add_function(fn):
    def wrapped(self, *args, **kwargs):
        
        #TODO: add assert, blocking and callback cannot be at the same time

        if self._ctrl._blocking:
            self._ctrl._asyncResults["blocking"] = AsyncResult()

        #execute function
        self._ctrl.send(self._ctrl._scope, fn.__name__, msg_type=self._msg_type)

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

        if self._ctrl._callback:
            self._ctrl._callback()

    return wrapped

class UPIRadio(object):
    def __init__(self, ctrl):
        self._ctrl = ctrl
        self._msg_type = "RADIO"

    @_add_function
    def set_channel(self, ch):
        '''
        set_channel 
        ch - channel to set
        '''
        pass
        

    @_add_function
    def set_power(self, pwr):
        '''
        func desc
        '''
        pass


    @_add_function
    def start_server(self):
        '''
        func desc
        '''
        pass