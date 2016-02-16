import logging
import subprocess
import zmq.green as zmq
import random

__author__ = "Piotr Gawlowicz, Mikolaj Chwalisz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "{gawlowicz, chwalisz}@tkn.tu-berlin.de"

class ControllerModule(object):
    def __init__(self, name, path, args):
        self.log = logging.getLogger("{module}.{name}".format(
            module=self.__class__.__module__, name=self.__class__.__name__))
        self.name = name
        self.path = path
        self.args = args
        self.port = None

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PAIR)

        self.start_server_for_module()
        self.start_module_process()
        pass

    def start_server_for_module(self):

        self.port = random.randint(5000, 10000)
        while True:
            try:
                self.socket.bind("tcp://*:%s" % self.port)
                break
            except:
                self.port = random.randint(5000, 10000)

        self.log.debug("Server for {} started on port: {} ".format(self.name, self.port))

    def start_module_process(self):
        cmd = [self.path,
               '--port', str(self.port)
               ]
        cmd.extend(filter(None, self.args))
        self.pid = subprocess.Popen(cmd)
        self.log.debug("Module: {}, with args: {}, PID: {} started".format(self.name, self.args, self.pid.pid))

    def exit(self):
        self.pid.kill()
        pass


    def send_msg_to_module(self, msgContainer):
        self.log.debug("Module: {} sends msg".format(self.name))
        self.socket.send_multipart(msgContainer)




class bind_function(object):
    def __init__(self, upiFunc):
        fname = upiFunc.__name__
        self.upi_fname = set([fname])

    def __call__(self, f):
        f._upi_fname = self.upi_fname
        return f


def build_module(module_class):
    original_methods = module_class.__dict__.copy()
    for name, method in original_methods.iteritems():
        if hasattr(method, '_upi_fname'):
            #create alias
            for falias in method._upi_fname - set(original_methods):
                setattr(module_class, falias, method)
    return module_class


class ControllerUpiModule(object):
    def __init__(self, controller):
        self.log = logging.getLogger("{module}.{name}".format(
            module=self.__class__.__module__, name=self.__class__.__name__))
        self.controller = controller