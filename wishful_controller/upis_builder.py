import logging
import inspect
import wishful_upis
import decorator
from collections import namedtuple
from gevent import Greenlet
from gevent.event import AsyncResult

__author__ = "Piotr Gawlowicz, Mikolaj Chwalisz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "{gawlowicz, chwalisz}@tkn.tu-berlin.de"

DefaultArgSpec = namedtuple('DefaultArgSpec', 'has_default default_value')

def _get_default_arg(args, defaults, arg_index):
    """ Method that determines if an argument has default value or not,
    and if yes what is the default value for the argument

    :param args: array of arguments, eg: ['first_arg', 'second_arg', 'third_arg']
    :param defaults: array of default values, eg: (42, 'something')
    :param arg_index: index of the argument in the argument array for which,
    this function checks if a default value exists or not. And if default value
    exists it would return the default value. Example argument: 1
    :return: Tuple of whether there is a default or not, and if yes the default
    value, eg: for index 2 i.e. for "second_arg" this function returns (True, 42)
    """
    if not defaults:
        return DefaultArgSpec(False, None)

    args_with_no_defaults = len(args) - len(defaults)

    if arg_index < args_with_no_defaults:
        return DefaultArgSpec(False, None)
    else:
        value = defaults[arg_index - args_with_no_defaults]
        if (type(value) is str):
            value = '"%s"' % value
        return DefaultArgSpec(True, value)

def get_method_sig(method):
    """ Given a function, it returns a string that pretty much looks how the
    function signature would be written in python.

    :param method: a python method
    :return: A string similar describing the pythong method signature.
    eg: "my_method(first_argArg, second_arg=42, third_arg='something')"
    """

    # The return value of ArgSpec is a bit weird, as the list of arguments and
    # list of defaults are returned in separate array.
    # eg: ArgSpec(args=['first_arg', 'second_arg', 'third_arg'],
    # varargs=None, keywords=None, defaults=(42, 'something'))
    argspec = inspect.getargspec(method)
    arg_index=0
    args = []

    # Use the args and defaults array returned by argspec and find out
    # which arguments has default
    for arg in argspec.args:
        default_arg = _get_default_arg(argspec.args, argspec.defaults, arg_index)
        if default_arg.has_default:
            args.append("%s=%s" % (arg, default_arg.default_value))
        else:
            args.append(arg)
        arg_index += 1
    return "%s(%s)" % (method.__name__, ", ".join(args))

@decorator.decorator
def _add_function(fn, *args, **kwargs):
    def wrapped(self, *args, **kwargs):
        #send function to controller
        return self._ctrl.send_cmd(upi_type=self._msg_type, fname=fn.__name__, args=args, kwargs=kwargs)
    return wrapped(*args, **kwargs)

class UpiBase(object):
    def __init__(self):
        self._ctrl = None
        self._msg_type = None

class UpiRadio(UpiBase):
    def __init__(self, ctrl):
        self._ctrl = ctrl
        self._msg_type = "radio"

class UpiNet(UpiBase):
    def __init__(self, ctrl):
        self._ctrl = ctrl
        self._msg_type = "net"

class UpiMgmt(UpiBase):
    def __init__(self, ctrl):
        self._ctrl = ctrl
        self._msg_type = "mgmt"


class UpiBuilder(object):
    def __init__(self, ctrl):
        self._ctrl = ctrl

    def perpare_function(self, name, function):
        # Based on
        # http://stackoverflow.com/questions/10303248/true-dynamic-and-anonymous-functions-possible-in-python
        # #http://code.activestate.com/recipes/577659-decorators-for-adding-aliases-to-methods-in-a-clas/
        # TODO: is it possible to make it better? change signature?
        # https://www.python.org/dev/peps/pep-0362/#visualizing-callable-objects-signature
        # https://docs.python.org/3/library/inspect.html#inspect.signature
        # support default
        # http://stackoverflow.com/questions/2677185/how-can-i-read-a-functions-signature-including-default-argument-values

        sig = get_method_sig(function)
        sig = sig.split("(", 1)
        sig = sig[0] + "(self, " + sig[1]
        code = "def {}: pass".format(sig)
        myGlobals = {}
        exec code in myGlobals
        function = myGlobals[name]

        decorated_fun = _add_function(function)
        return decorated_fun

    def create_radio(self):
        for method in dir(wishful_upis.radio):
            if callable(getattr(wishful_upis.radio, method)):
                function = getattr(wishful_upis.radio, method)
                function = self.perpare_function(method, function)
                setattr(UpiRadio, method, function)
        radio = UpiRadio(self._ctrl)
        return radio

    def create_net(self):
        for method in dir(wishful_upis.net):
            if callable(getattr(wishful_upis.net, method)):
                function = getattr(wishful_upis.net, method)
                function = self.perpare_function(method, function)
                setattr(UpiNet, method, function)
        net = UpiNet(self._ctrl)
        return net

    def create_mgmt(self):
        for method in dir(wishful_upis.mgmt):
            if callable(getattr(wishful_upis.mgmt, method)):
                function = getattr(wishful_upis.mgmt, method)
                function = self.perpare_function(method, function)
                setattr(UpiMgmt, method, function)
        mgmt = UpiMgmt(self._ctrl)
        return mgmt