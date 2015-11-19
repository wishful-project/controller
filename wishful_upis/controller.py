import logging
import time
import sys
import zmq
import uuid
import yaml
import datetime
from controller_module import *
from msgs.management_pb2 import *
from msgs.msg_helper import get_msg_type

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
        self.ul_socket.setsockopt(zmq.SUBSCRIBE,  "NEW_NODE")
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
        # TODO: specific (named) socket for synchronization and discovery modules or do we need it ?
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
        msgDesc = MsgDesc()
        msgDesc.ParseFromString(msgContainer[1])
        msg = NewNodeMsg()
        msg.ParseFromString(msgContainer[2])
        agentId = str(msg.agent_uuid)
        agentName = msg.name
        agentInfo = msg.info

        self.log.debug("Controller adds new node with UUID: {0}, Name: {1}, Info: {2}".format(agentId,agentName,agentInfo))
        self.nodes.append(agentId)
        self.ul_socket.setsockopt(zmq.SUBSCRIBE,  str(agentId))

        group = agentId
        msgDesc.Clear()
        msgDesc.msg_type = get_msg_type(NewNodeAck)
        msg = NewNodeAck()
        msg.status = True
        msg.controller_uuid = self.myId
        msg.agent_uuid = agentId
        msg.topics.append("ALL")

        msgContainer = [group, msgDesc.SerializeToString(), msg.SerializeToString()]

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
                msgDesc = MsgDesc()
                msgDesc.ParseFromString(msgContainer[1])
                msg = msgContainer[2]

                self.log.debug("Controller received message: {0} from agent".format(msgDesc.msg_type))
                if msgDesc.msg_type == get_msg_type(NewNodeMsg):
                    self.add_new_node(msgContainer)
                else:
                    self.log.debug("Controller drops unknown message: {0} from agent".format(msgDesc.msg_type))

            self.log.debug("Sends new command")

            if i % 2 == 1:
                group = self.nodes[0]
                msgDesc.Clear()
                msgDesc.msg_type = "RADIO"
                msg = "SET_CHANNEL"
            else:
                group = self.nodes[0]
                msgDesc.Clear()
                msgDesc.msg_type = "PERFORMANCE_TEST"
                msgDesc.exec_time = str(datetime.datetime.now() + datetime.timedelta(seconds=2))                
                msg = "START_SERVER"

            i += 1

            self.log.debug("Controller sends message: {0}::{1}".format(msgDesc.msg_type, msg))
            msgContainer = [group, msgDesc.SerializeToString(), msg]
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
