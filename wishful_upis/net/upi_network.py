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

class UPINetwork(object):
    def __init__(self, ctrl):
        self._ctrl = ctrl

    @_clean_after_use
    def start_server(self):
	    msg_type = "PERFORMANCE_TEST"
	    msg = "START_SERVER"
        self._ctrl.send(None, msg, msg_type=msg_type)