import logging
import zmq
import random
import sys
import time
from wishful_framework.modules import *

__author__ = "Piotr Gawlowicz, Mikolaj Chwalisz"
__copyright__ = "Copyright (c) 2015, Technische Universität Berlin"
__version__ = "0.1.0"
__email__ = "{gawlowicz, chwalisz}@tkn.tu-berlin.de"


class ControllerModule(WishfulModule):
    def __init__(self):
        super(ControllerModule, self).__init__()


    def send_to_module(self, msgContainer):
        self.log.debug("Module {} received cmd".format(self.__class__.__name__))
        pass