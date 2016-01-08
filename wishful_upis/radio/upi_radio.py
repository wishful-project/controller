import logging

__author__ = "Piotr Gawlowicz, Mikolaj Chwalisz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "{gawlowicz, chwalisz}@tkn.tu-berlin.de"

def _clean_after_use(fn):
    def wrapped(self, *args, **kwargs):
        fn(self, *args, **kwargs)
        self._ctrl._scope = None
        self._ctrl._exec_time = None
        self._ctrl._delay = None        
    return wrapped

class UPIRadio(object):
    def __init__(self, ctrl):
        self._ctrl = ctrl
        self._msg_type = "radio"

    @_clean_after_use
    def set_channel(self, ch=None):
        msg_type = "RADIO" 
        msg = "SET_CHANNEL"
        self._ctrl.send(None, msg, msg_type=msg_type)

    @_clean_after_use
    def set_power(self, pwr=None):
        return 0

    @_clean_after_use
    def start_server(self):
        msg_type = "PERFORMANCE_TEST"
        msg = "START_SERVER"
        self._ctrl.send(None, msg, msg_type=msg_type)