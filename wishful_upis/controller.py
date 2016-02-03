import logging
import time
import sys
#import zmq
import zmq.green as zmq
import uuid
import yaml
import datetime
import gevent
from gevent import Greenlet
from gevent.event import AsyncResult

from controller_module import *
from msgs.management_pb2 import *
from msgs.msg_helper import get_msg_type

__author__ = "Piotr Gawlowicz, Mikolaj Chwalisz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "{gawlowicz, chwalisz}@tkn.tu-berlin.de"

        
class Controller(Greenlet):
    def __init__(self, dl, ul):
        Greenlet.__init__(self)
        self.log = logging.getLogger("{module}.{name}".format(
            module=self.__class__.__module__, name=self.__class__.__name__))

        self.config = None
        self.myUuid = uuid.uuid4()
        self.myId = str(self.myUuid)
        self.modules = {}

        self.callbacks = {}

        self.nodes = []
        self.groups = []
        self.msg_type = {} # 'full name': [(group, callback)]
        self.echoMsgInterval = 3

        self.context = zmq.Context()
        self.poller = zmq.Poller()

        self.ul_socket = self.context.socket(zmq.SUB) # one SUB socket for uplink communication over topics
        self.ul_socket.setsockopt(zmq.SUBSCRIBE,  "NEW_NODE")
        self.ul_socket.setsockopt(zmq.SUBSCRIBE,  "NODE_EXIT")
        self.ul_socket.setsockopt(zmq.SUBSCRIBE,  "RESPONSE")
        self.ul_socket.bind(ul)

        self.dl_socket = self.context.socket(zmq.PUB) # one PUB socket for downlink communication over topics
        self.dl_socket.bind(dl)

        #register UL socket in poller
        self.poller.register(self.ul_socket, zmq.POLLIN)

    def stop(self):
        self.log.debug("Exit all modules' subprocesses")
        for name, module in self.modules.iteritems():
            module.exit()
        self.ul_socket.setsockopt(zmq.LINGER, 0)
        self.dl_socket.setsockopt(zmq.LINGER, 0)
        self.ul_socket.close()
        self.dl_socket.close()
        self.context.term()

    def _run(self):
        self.log.debug("Controller starts".format())
 
        self.running = True
        while self.running:
            try:
                self.process_msgs()
            except KeyboardInterrupt:
                self.log.debug("Controller exits")
            except:
                self.log.debug("Unexpected error:".format(sys.exc_info()[0]))
            finally:
                self.stop()


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

    def add_msg_callback(self, msg_type, group, **options):
        def decorator(callback):
            self.set_callback(msg_type, group, callback, **options)
            return callback
        return decorator

    def set_callback(self, msg_type, group, callback, **options):
        self.callbacks[msg_type] = callback

    def add_new_node(self, msgContainer):
        group = msgContainer[0]
        msgDesc = MsgDesc()
        msgDesc.ParseFromString(msgContainer[1])
        msg = NewNodeMsg()
        msg.ParseFromString(msgContainer[2])
        agentId = str(msg.agent_uuid)
        agentName = msg.name
        agentInfo = msg.info

        if agentId in self.nodes:
            self.log.debug("Already known Agent UUID: {0}, Name: {1}, Info: {2}".format(agentId,agentName,agentInfo))
            return

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

    def remove_new_node(self, msgContainer):
        group = msgContainer[0]
        msgDesc = MsgDesc()
        msgDesc.ParseFromString(msgContainer[1])
        msg = NodeExitMsg()
        msg.ParseFromString(msgContainer[2])
        agentId = str(msg.agent_uuid)
        reason = msg.reason

        self.log.debug("Controller removes new node with UUID: {0}, Reason: {1}".format(agentId, reason))
        if agentId in self.nodes:
            self.nodes.remove(agentId)

    def send_hello_msg_to_controller(self, nodeId):
        self.log.debug("Controller sends HelloMsg to agent")
        group = nodeId
        msgDesc = MsgDesc()
        msgDesc.msg_type = get_msg_type(HelloMsg)
        msg = HelloMsg()
        msg.uuid = str(self.myId)
        msg.timeout = 3 * self.echoMsgInterval
        msgContainer = [group, msgDesc.SerializeToString(), msg.SerializeToString()]
        self.dl_socket.send_multipart(msgContainer)

    def serve_hello_msg(self, msgContainer):
        self.log.debug("Controller received HELLO MESSAGE from agent".format())
        group = msgContainer[0]
        msgDesc = MsgDesc()
        msgDesc.ParseFromString(msgContainer[1])
        msg = HelloMsg()
        msg.ParseFromString(msgContainer[2])

        self.send_hello_msg_to_controller(str(msg.uuid))
        #reschedule agent delete function in scheduler
        pass

    def send(self, group, msg, msg_type=None, delay=None, exec_time=None):
        self.log.debug("Controller Sends message".format())

        if group in self.nodes or group in self.groups:
            msgDesc = MsgDesc()
            
            if msg_type:  
                msgDesc.msg_type = msg_type
                #TODO: else get msg type from msg but check if not string

            if delay:
                msgDesc.exec_time = str(datetime.datetime.now() + datetime.timedelta(seconds=delay))

            if exec_time:
                msgDesc.exec_time = str(exec_time)

            self.log.debug("Controller sends message: {0}::{1}::{2}".format(group, msgDesc.msg_type, msg))
            msgContainer = []
            msgContainer.append(str(group))
            msgContainer.append(msgDesc.SerializeToString())
            msgContainer.append(msg)
            self.dl_socket.send_multipart(msgContainer)


    def process_msgs(self):
        while True:
            socks = dict(self.poller.poll())
            if self.ul_socket in socks and socks[self.ul_socket] == zmq.POLLIN:
                try:
                    msgContainer = self.ul_socket.recv_multipart(zmq.NOBLOCK)
                except zmq.ZMQError:
                    break

                assert len(msgContainer) == 3
                group = msgContainer[0]
                msgDesc = MsgDesc()
                msgDesc.ParseFromString(msgContainer[1])
                msg = msgContainer[2]

                self.log.debug("Controller received message: {0} from agent".format(msgDesc.msg_type))
                if msgDesc.msg_type == get_msg_type(NewNodeMsg):
                    self.add_new_node(msgContainer)
                elif msgDesc.msg_type == get_msg_type(HelloMsg):
                    self.serve_hello_msg(msgContainer)
                elif msgDesc.msg_type == get_msg_type(NodeExitMsg):
                    self.remove_new_node(msgContainer)
                else:
                    self.log.debug("Controller drops unknown message: {0} from agent".format(msgDesc.msg_type))

                if msgDesc.msg_type in self.callbacks:
                    self.callbacks[msgDesc.msg_type](group, msgDesc.uuid, msg)


    def test_run(self):
        self.log.debug("Controller starts".format())
        try:
            self.process_msgs()

        except KeyboardInterrupt:
            self.log.debug("Controller exits")

        except:
             self.log.debug("Unexpected error:".format(sys.exc_info()[0]))
        finally:
            self.log.debug("Exit all modules' subprocesses")
            for name, module in self.modules.iteritems():
                module.exit()
            self.ul_socket.setsockopt(zmq.LINGER, 0)
            self.dl_socket.setsockopt(zmq.LINGER, 0)
            self.ul_socket.close()
            self.dl_socket.close()
            self.context.term()
