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
import wishful_framework as msgs
import upis

__author__ = "Piotr Gawlowicz, Mikolaj Chwalisz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "{gawlowicz, chwalisz}@tkn.tu-berlin.de"

#TODO: improve hello sending, add scheduler + timeout mechanism for node removal, 
#and in agent check source_ID if still conntedted to the same controller, in case of reboot
#agent should reinitiate discovery procedure and connect once again in case of controller reboot (new UUID)

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
        self.newNodeCallback = None
        self.nodeExitCallback = None
        self.call_id_gen = 0

        self._nodes = []
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

        #UPIs
        builder = upis.upis_builder.UpiBuilder(self)
        self.radio = builder.create_radio()
        self.net = builder.create_net()
        self.mgmt = builder.create_mgmt()

        self._scope = None
        self._exec_time = None
        self._delay = None
        self._timeout = None
        self._blocking = False
        self._callback = None
        #container for blocking calls
        self._asyncResults = {}

    def stop(self):
        self.running = False
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
            self.process_msgs()

    def nodes(self, nodelist):
        self._scope = nodelist
        return self

    def exec_time(self, exec_time):
        self._exec_time = exec_time
        return self

    def delay(self, delay):
        self._delay = delay
        return self

    def timeout(self, value):
        self._timeout = value
        return self

    def blocking(self, value=False):
        self._blocking = value
        return self

    def callback(self, callback):
        self._callback = callback
        return self

    def read_config_file(self, path=None):
        self.log.debug("Path to module: {}".format(path))

        with open(path, 'r') as f:
           config = yaml.load(f)

        return config

    def load_modules(self, config):
        self.log.debug("Config: {}".format(config))
        pass

    def load_modules(self, config):
        self.log.debug("Config: {}".format(config))

        for module_name, module_parameters in config.iteritems():
            self.add_module(
                self.exec_module(
                        name=module_name,
                        path=module_parameters['path'],
                        args=module_parameters['args']
                )
            )

    def add_module(self, module):
        self.log.debug("Adding new module: {}".format(module))
        self.modules[module.name] = module

        #register module socket in poller
        # TODO: specific (named) socket for synchronization and discovery modules or do we need it ?
        #self.poller.register(module.socket, zmq.POLLIN)


    def exec_module(self, name, path, args):
        new_module = ControllerModule(name, path, args)
        return new_module

    def my_import(self, module_name):
        pyModule = __import__(module_name)
        globals()[module_name] = pyModule
        return pyModule

    def add_upi_module(self, upi, moduleName, className, importAs):
        self.log.debug("Adding new UPI module: {}:{}:{}".format(moduleName, className, importAs))
        pyModule = self.my_import(moduleName)
        moduleContructor = getattr(pyModule, className)
        newModule = moduleContructor(self)
        setattr(self, importAs, newModule)

    def new_node_callback(self, **options):
        def decorator(callback):
            self.newNodeCallback = callback
            return callback
        return decorator

    def node_exit_callback(self, **options):
        def decorator(callback):
            self.nodeExitCallback = callback
            return callback
        return decorator

    def add_callback(self, group, function, **options):
        def decorator(callback):
            self.log.debug("Register callback for: ", function.__name__)
            self.callbacks[function.__name__] = callback
            return callback
        return decorator

    def add_new_node(self, msgContainer):
        group = msgContainer[0]
        cmdDesc = msgs.CmdDesc()
        cmdDesc.ParseFromString(msgContainer[1])
        msg = msgs.NewNodeMsg()
        msg.ParseFromString(msgContainer[2])
        agentId = str(msg.agent_uuid)
        agentName = msg.name
        agentInfo = msg.info
        #TODO: add supported cmd list

        if agentId in self._nodes:
            self.log.debug("Already known Agent UUID: {}, Name: {}, Info: {}".format(agentId,agentName,agentInfo))
            return

        if self.newNodeCallback:
            self.newNodeCallback(group, agentId, agentName, agentInfo)

        self.log.debug("Controller adds new node with UUID: {}, Name: {}, Info: {}".format(agentId,agentName,agentInfo))
        self._nodes.append(agentId)
        self.ul_socket.setsockopt(zmq.SUBSCRIBE,  str(agentId))

        group = agentId
        cmdDesc.Clear()
        cmdDesc.type = msgs.get_msg_type(msgs.NewNodeAck)
        cmdDesc.func_name = msgs.get_msg_type(msgs.NewNodeAck)
        msg = msgs.NewNodeAck()
        msg.status = True
        msg.controller_uuid = self.myId
        msg.agent_uuid = agentId
        msg.topics.append("ALL")

        msgContainer = [group, cmdDesc.SerializeToString(), msg.SerializeToString()]

        time.sleep(1) # TODO: why?
        self.dl_socket.send_multipart(msgContainer)

    def remove_new_node(self, msgContainer):
        group = msgContainer[0]
        cmdDesc = msgs.CmdDesc()
        cmdDesc.ParseFromString(msgContainer[1])
        msg = msgs.NodeExitMsg()
        msg.ParseFromString(msgContainer[2])
        agentId = str(msg.agent_uuid)
        reason = msg.reason

        if self.nodeExitCallback:
            self.nodeExitCallback(group, agentId, reason)

        self.log.debug("Controller removes new node with UUID: {}, Reason: {}".format(agentId, reason))
        if agentId in self._nodes:
            self._nodes.remove(agentId)

    def send_hello_msg_to_controller(self, nodeId):
        self.log.debug("Controller sends HelloMsg to agent")
        group = nodeId
        cmdDesc = msgs.CmdDesc()
        cmdDesc.type = msgs.get_msg_type(msgs.HelloMsg)
        cmdDesc.func_name = msgs.get_msg_type(msgs.HelloMsg)
        msg = msgs.HelloMsg()
        msg.uuid = str(self.myId)
        msg.timeout = 3 * self.echoMsgInterval
        msgContainer = [group, cmdDesc.SerializeToString(), msg.SerializeToString()]
        self.dl_socket.send_multipart(msgContainer)

    def serve_hello_msg(self, msgContainer):
        self.log.debug("Controller received HELLO MESSAGE from agent".format())
        group = msgContainer[0]
        cmdDesc = msgs.CmdDesc()
        cmdDesc.ParseFromString(msgContainer[1])
        msg = msgs.HelloMsg()
        msg.ParseFromString(msgContainer[2])

        self.send_hello_msg_to_controller(str(msg.uuid))
        #TODO: reschedule agent delete function in scheduler, support aspscheduler first
        pass

    def generate_call_id(self):
        self.call_id_gen = self.call_id_gen + 1
        return self.call_id_gen

    def send(self, group, upi_type, fname, delay=None, exec_time=None, timeout=None, blocking=False, *args, **kwargs):
        self.log.debug("Controller calls {}.{} with args:{}, kwargs:{}".format(upi_type, fname, args, kwargs))

        if not group:
            group = self._scope
        if not exec_time:
            exec_time = self._exec_time
        if not delay:
            delay = self._delay

        callId = None
        if group in self._nodes or group in self.groups:
            cmdDesc = msgs.CmdDesc()
            cmdDesc.type = upi_type
            cmdDesc.func_name = fname
            callId = str(self.generate_call_id())
            cmdDesc.call_id = callId

            if blocking:
                self._asyncResults[callId] = AsyncResult()       

            #TODO: support timeout, on controller and agent sides?

            if delay:
                cmdDesc.exec_time = str(datetime.datetime.now() + datetime.timedelta(seconds=delay))

            if exec_time:
                cmdDesc.exec_time = str(exec_time)

            self.log.debug("Controller sends message: {}:{}:{}".format(group, cmdDesc.type, cmdDesc.func_name))
            msgContainer = []
            msgContainer.append(str(group))
            msgContainer.append(cmdDesc.SerializeToString())
            #TODO: send args in third part of message
            msgContainer.append(fname)
            self.dl_socket.send_multipart(msgContainer)
        return callId


    def process_msgs(self):
        socks = dict(self.poller.poll())
        if self.ul_socket in socks and socks[self.ul_socket] == zmq.POLLIN:
            try:
                msgContainer = self.ul_socket.recv_multipart(zmq.NOBLOCK)
            except zmq.ZMQError:
                raise zmq.ZMQError

            assert len(msgContainer) == 3
            group = msgContainer[0]
            cmdDesc = msgs.CmdDesc()
            cmdDesc.ParseFromString(msgContainer[1])
            msg = msgContainer[2]

            self.log.debug("Controller received message: {} from agent".format(cmdDesc.type))
            if cmdDesc.type == msgs.get_msg_type(msgs.NewNodeMsg):
                self.add_new_node(msgContainer)
            elif cmdDesc.type == msgs.get_msg_type(msgs.HelloMsg):
                self.serve_hello_msg(msgContainer)
            elif cmdDesc.type == msgs.get_msg_type(msgs.NodeExitMsg):
                self.remove_new_node(msgContainer)
            else:
                self.log.debug("Controller received message: {}:{} from agent".format(cmdDesc.type, cmdDesc.func_name))

                #get call_id 
                callId = cmdDesc.call_id
                if callId in self._asyncResults:
                    self._asyncResults[callId].set(msg)
                else:
                    if cmdDesc.call_id in self.callbacks:
                        self.callbacks[cmdDesc.call_id](group, cmdDesc.caller_id, msg)
                        del self.callbacks[cmdDesc.call_id]
                    elif cmdDesc.func_name in self.callbacks:
                        self.callbacks[cmdDesc.func_name](group, cmdDesc.caller_id, msg)
                    else:
                        #TODO: else default callback
                        pass


    def test_run(self):
        self.log.debug("Controller starts".format())
        
        if 0:
            self.process_msgs()
        else:
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