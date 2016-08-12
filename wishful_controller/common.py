from gevent.local import local
import wishful_upis as upis
from wishful_framework import upis_builder

__author__ = "Piotr Gawlowicz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "gawlowicz@tkn.tu-berlin.de"


class CallingContext(local):
    def __init__(self):
        # function call context
        self._scope = None
        self._iface = None
        self._exec_time = None
        self._delay = None
        self._timeout = None
        self._blocking = True
        self._callback = None
        self._upi_type = None
        self._upi = None
        self._args = None
        self._kwargs = None


class ControllableUnit(object):
    def __init__(self):
        self._callingCtx = CallingContext()
        self._clear_call_context()
        # UPIs
        builder = upis_builder.UpiBuilder(self)
        self.radio = builder.create_upi(upis.radio.Radio, "radio")
        self.net = builder.create_upi(upis.net.Network, "net")
        self.mgmt = builder.create_upi(upis.mgmt.Mgmt, "mgmt")
        self.context = builder.create_upi(upis.context.Context, "context")

    def group(self, group):
        self._callingCtx._scope = group
        return self

    def node(self, node):
        self._callingCtx._scope = node
        return self

    def nodes(self, nodelist):
        self._callingCtx._scope = nodelist
        return self

    def iface(self, iface):
        self._callingCtx._iface = iface
        return self

    def exec_time(self, exec_time):
        self._callingCtx._exec_time = exec_time
        return self

    def delay(self, delay):
        self._callingCtx._delay = delay
        return self

    def timeout(self, value):
        self._callingCtx._timeout = value
        return self

    def blocking(self, value=True):
        self._callingCtx._blocking = value
        return self

    def callback(self, callback):
        self._callingCtx._callback = callback
        return self

    def _clear_call_context(self, ctx=None):
        self._callingCtx._scope = None
        self._callingCtx._iface = None
        self._callingCtx._exec_time = None
        self._callingCtx._delay = None
        self._callingCtx._timeout = None
        self._callingCtx._blocking = True
        self._callingCtx._callback = None
        self._callingCtx._upi = None
        self._callingCtx._args = None
        self._callingCtx._kwargs = None

        if ctx:
            ctx._scope = None
            ctx._iface = None
            ctx._exec_time = None
            ctx._delay = None
            ctx._timeout = None
            ctx._blocking = True
            ctx._callback = None
            ctx._upi = None
            ctx._args = None
            ctx._kwargs = None

    def send_msg(self, ctx):
        pass

    def cmd_wrapper(self, upi_type, fname, *args, **kwargs):
        self._callingCtx._upi_type = upi_type
        self._callingCtx._upi = fname
        self._callingCtx._args = args
        self._callingCtx._kwargs = kwargs
        return self.send_msg(self._callingCtx)

    def start_event(self, event, ctx=None):
        self._callingCtx._upi_type = event.__class__.__module__.split('.')[-1]
        self._callingCtx._upi = event.__class__.__name__
        self._callingCtx._args = ["start"]
        self._callingCtx._kwargs = {}
        return self.send_msg(self._callingCtx)

    def stop_event(self, event, ctx=None):
        self._callingCtx._upi_type = event.__class__.__module__.split('.')[-1]
        self._callingCtx._upi = event.__class__.__name__
        self._callingCtx._args = ["stop"]
        self._callingCtx._kwargs = {}
        return self.send_msg(self._callingCtx)

    def start_service(self, service, ctx=None):
        self._callingCtx._upi_type = service.__module__.split('.')[-1]
        self._callingCtx._upi = service.__class__.__name__
        self._callingCtx._args = ["start"]
        self._callingCtx._kwargs = {}
        return self.send_msg(self._callingCtx)

    def stop_service(self, service, ctx=None):
        self._callingCtx._upi_type = service.__module__.split('.')[-1]
        self._callingCtx._upi = service.__class__.__name__
        self._callingCtx._args = ["stop"]
        self._callingCtx._kwargs = {}
        return self.send_msg(self._callingCtx)
