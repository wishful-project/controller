import logging
from .common import ControllableUnit

__author__ = "Piotr Gawlowicz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "gawlowicz@tkn.tu-berlin.de"


class Interface(ControllableUnit):
    def __init__(self, name, node):
        super().__init__()
        self._log = logging.getLogger("{module}.{name}".format(
            module=self.__class__.__module__, name=self.__class__.__name__))
        self._name = name
        self._node = node

    def exec_cmd(self, ctx):
        ctx._iface = self._name
        response = self._node.exec_cmd(ctx)
        self._clear_call_context()
        return response
