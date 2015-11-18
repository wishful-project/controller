import logging
import time
import sys
import zmq
import uuid
import yaml
from controller_module import *
from msgs.management_pb2 import Description as msgDesc

__author__ = "Piotr Gawlowicz, Mikolaj Chwalisz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "{gawlowicz, chwalisz}@tkn.tu-berlin.de"

class Controller(object):
    def __init__(self, dl, ul):
        self.log = logging.getLogger("{module}.{name}".format(
            module=self.__class__.__module__, name=self.__class__.__name__))

        self.config = None
        self.myUuid = uuid.uuid4()
        self.myId = str(self.myUuid)
        self.modules = {}

        self.nodes = []
        self.groups = []
        self.msg_type = {} # 'full name': [(group, callback)]

        self.context = zmq.Context()
        self.poller = zmq.Poller()

        self.ul_socket = self.context.socket(zmq.SUB) # one SUB socket for uplink communication over topics
        self.ul_socket.setsockopt(zmq.SUBSCRIBE,  "NEW_NODE_MSG")
        self.ul_socket.setsockopt(zmq.SUBSCRIBE,  "RESPONSE")
        self.ul_socket.bind(ul)

        self.dl_socket = self.context.socket(zmq.PUB) # one PUB socket for downlink communication over topics
        self.dl_socket.bind(dl)

        #register UL socket in poller
        self.poller.register(self.ul_socket, zmq.POLLIN)

    def read_config_file(self, path=None):
        self.log.debug("Path to module: {0}".format(path))

        with open(path, 'r') as f:
           config = yaml.load(f)

        return config

    def load_modules(self, config):
        self.log.debug("Config: {0}".format(config))
        pass

    def load_modules(self, config):
        self.log.debug("Config: {0}".format(config))

        for module_name, module_parameters in config.iteritems():
            self.add_module(
                self.exec_module(
                        name=module_name,
                        path=module_parameters['path'],
                        args=module_parameters['args']
                )
            )

    def add_module(self, module):
        self.log.debug("Adding new module: {0}".format(module))
        self.modules[module.name] = module

        #register module socket in poller
        # TODO: specific (named) socket for synchronization and discovery modules
        #self.poller.register(module.socket, zmq.POLLIN)


    def exec_module(self, name, path, args):
        new_module = ControllerModule(name, path, args)
        return new_module

    def add_callback(self, group, callback):
        # assert callable(callback)
        self.log.debug("Added callback {:s} for group {:s}".format(callback, group))
        pass

    def add_new_node(self, msgContainer):
        group = msgContainer[0]
        msgType = msgDesc()
        msgType.ParseFromString(msgContainer[1])
        msg = msgContainer[2]

        nodeId = msg
        self.log.debug("Controller adds new node with UUID: {0}".format(nodeId))
        self.nodes.append(nodeId)
        self.ul_socket.setsockopt(zmq.SUBSCRIBE,  nodeId)

        group = nodeId
        msgType.Clear()
        msgType.msg_type = "NEW_NODE_ACK"
        msgType.exec_time = 0
        msg = "OK_OK_OK"
        msgContainer = [group, msgType.SerializeToString(), msg]

        time.sleep(1) # TODO: why?
        self.dl_socket.send_multipart(msgContainer)

    def process_msgs(self):
        i = 0
        while True:
            socks = dict(self.poller.poll())

            if self.ul_socket in socks and socks[self.ul_socket] == zmq.POLLIN:
                msgContainer = self.ul_socket.recv_multipart()

                assert len(msgContainer) == 3
                group = msgContainer[0]
                msgType = msgDesc()
                msgType.ParseFromString(msgContainer[1])
                msg = msgContainer[2]

                self.log.debug("Controller received message: {0}::{1} from agent".format(msgType.msg_type, msg))

                if msgType.msg_type == "NEW_NODE_MSG":
                    self.add_new_node(msgContainer)
                else:
                    self.log.debug("Controller drops unknown message: {0}::{1} from agent".format(msgType.msg_type, msg))

            self.log.debug("Sends new command")

            if i % 2 == 1:
                group = self.nodes[0]
                msgType.Clear()
                msgType.msg_type = "RADIO"
                msgType.exec_time = 0
                msg = "SET_CHANNEL"
            else:
                group = self.nodes[0]
                msgType.Clear()
                msgType.msg_type = "PERFORMANCE_TEST"
                msgType.exec_time = 2                
                msg = "START_SERVER"

            i += 1
            #a = datetime.datetime.now()
            #string_date = "2013-09-28 20:30:55.78200"
            #b = datetime.datetime.strptime(string_date, "%Y-%m-%d %H:%M:%S.%f")
            #if a > b:
            #   print "OK"

            self.log.debug("Controller sends message: {0}::{1}".format(msgType.msg_type, msg))
            msgContainer = [group, msgType.SerializeToString(), msg]
            self.dl_socket.send_multipart(msgContainer)

    def run(self):
        self.log.debug("Controller starts".format())
        try:
            self.process_msgs()

        except KeyboardInterrupt:
            self.log.debug("Controller exits")

        finally:
            self.log.debug("Unexpected error:".format(sys.exc_info()[0]))
            self.log.debug("Kills all modules' subprocesses")
            for name, module in self.modules.iteritems():
                module.kill_module_subprocess()
            self.ul_socket.close()
            self.dl_socket.close()
            self.context.term()
