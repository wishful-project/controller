import logging
import inspect
import wishful_upis
import imp
import decorator

__author__ = "Piotr Gawlowicz, Mikolaj Chwalisz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "{gawlowicz, chwalisz}@tkn.tu-berlin.de"

#http://code.activestate.com/recipes/577659-decorators-for-adding-aliases-to-methods-in-a-clas/

@decorator.decorator
def _add_function(fn, *args, **kwargs):
    def wrapped(self, *args, **kwargs):
        print dir(self)
        print self.__dict__
        print args
        print kwargs
        print self._ctrl
        print self._msg_type
        return wrapped
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

    def create_radio(self):
        
        for method in dir(wishful_upis.radio):
            if callable(getattr(wishful_upis.radio, method)):
                function = getattr(wishful_upis.radio, method)

                # Based on:
                # http://stackoverflow.com/questions/10303248/true-dynamic-and-anonymous-functions-possible-in-python
                # TODO: is it possible to make it better? change signature?
                # https://www.python.org/dev/peps/pep-0362/#visualizing-callable-objects-signature
                
                fargs = inspect.getargspec(function).args
                fargs_str = ",".join(fargs)
                code = """def {}(self,{}): pass
                """.format(method,fargs_str)

                module = imp.new_module('myfunctions')
                exec code in module.__dict__
                function = getattr(module, method)
                del module

                decorated_fun = _add_function(function)
                setattr(UpiRadio, method, decorated_fun)
        radio = UpiRadio(self._ctrl)
        return radio

    def create_net(self):
        for method in dir(wishful_upis.net):
            if callable(getattr(wishful_upis.net, method)):
                print method
                function = getattr(wishful_upis.net, method)
                setattr(UpiRadio, method, classmethod(function))
        net = UpiNet(self._ctrl)
        return net



if __name__ == "__main__":
    builder = UpiBuilder(1)
    radio = builder.create_radio()
    print(inspect.getargspec(radio.set_channel))
    print radio.set_channel
    radio.set_channel(channel=1)