import logging
import wishful_upis

__author__ = "Piotr Gawlowicz, Mikolaj Chwalisz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "{gawlowicz, chwalisz}@tkn.tu-berlin.de"

#http://code.activestate.com/recipes/577659-decorators-for-adding-aliases-to-methods-in-a-clas/

class UpiBuilder(object):
    def __init__(self, ctrl):
        self._ctrl = ctrl

    def create_radio(self):
        for method in dir(wishful_upis.radio):
            if callable(getattr(wishful_upis.radio, method)):
                print method
                function = getattr(wishful_upis.radio, method)
                setattr(UpiRadio, method, staticmethod(function))
        radio = UpiRadio(self._ctrl)
        return radio

    def create_net(self):
        for method in dir(wishful_upis.net):
            if callable(getattr(wishful_upis.net, method)):
                print method
                function = getattr(wishful_upis.net, method)
                setattr(UpiRadio, method, staticmethod(function))
        net = UpiNet(self._ctrl)
        return net


class UpiRadio(object):
    def __init__(self, ctrl):
        self._ctrl = ctrl
        self._msg_type = "radio"


class UpiNet(object):
    def __init__(self, ctrl):
        self._ctrl = ctrl
        self._msg_type = "net"