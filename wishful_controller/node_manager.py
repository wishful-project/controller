import logging
import time
import sys
import gevent
import wishful_framework as msgs

__author__ = "Piotr Gawlowicz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "gawlowicz@tkn.tu-berlin.de"


class Group(object):
    def __init__(self, name):
        self.name = name
        self.uuid = str(uuid.uuid4())
        self.nodes = []

    def add_node(self, node):
        self.nodes.append(node)

    def remove_node(self,node):
        self.nodes.remove(node)


class Node(object):
    def __init__(self,msg):
        self.id = str(msg.agent_uuid)
        self.ip = str(msg.ip)
        self.name = str(msg.name)
        self.info = str(msg.info)
        self.modules = {}
        self.functions = {}
        self.interfaces = {}
        self.iface_to_modules = {}

        self._stop = False
        self._helloTimeout = 9
        self._timerCallback = None

        for module in msg.modules:
            self.modules[module.id] = str(module.name)
            for func in module.functions:
                if module.id not in self.functions:
                    self.functions[module.id] = [str(func.name)]
                else:
                    self.functions[module.id].append(str(func.name))

        for iface in msg.interfaces:
            self.interfaces[iface.id] = str(iface.name)
            for module in iface.modules:
                if iface.id in self.iface_to_modules:
                    self.iface_to_modules[iface.id].append(str(module.id))
                else:
                    self.iface_to_modules[iface.id] = [str(module.id)]

    def __str__(self):
        string = "ID: {} \nIP: {} \nName: {} \nInfo: {} \nModules: {} \
                  \nModule_Functions: {} \nInterfaces: {} \nIface_Modules {} \
                  ".format(self.id, self.ip, self.name, self.info, self.modules, self.functions, self.interfaces, self.iface_to_modules)
        return string

    def set_timer_callback(self, cb):
        self._timerCallback = cb

    def hello_timer(self):
        while not self._stop and self._helloTimeout:
            gevent.sleep(1)
            self._helloTimeout = self._helloTimeout - 1

        #remove node
        self._timerCallback(self)

    def refresh_hello_timer(self):
        self._helloTimeout = 9


class NodeManager(object):
    def __init__(self, controller):
        self.log = logging.getLogger("{module}.{name}".format(
            module=self.__class__.__module__, name=self.__class__.__name__))

        self.controller = controller
        self.nodes = []
        self.groups = []

        self.newNodeCallback = None
        self.nodeExitCallback = None

        self.helloMsgInterval = 3
        self.helloTimeout = 3*self.helloMsgInterval


    def get_node_by_id(self, nid):
        node = None
        for n in self.nodes:
            if n.id == nid:
                node = n;
                break
        return node 


    def get_node_by_ip(self, ip):
        node = None
        for n in self.nodes:
            if n.ip == ip:
                node = n;
                break
        return node


    def get_node_by_str(self, string):
        if isinstance(string, Node):
            return string

        node = None
        node = self.get_node_by_ip(string)
        if node:
            return node

        node = self.get_node_by_id(string)
        return node


    def add_node(self, msgContainer):
        topic = msgContainer[0]
        cmdDesc = msgContainer[1]
        msg = msgs.NewNodeMsg()
        msg.ParseFromString(msgContainer[2])
        agentId = str(msg.agent_uuid)
        agentName = msg.name
        agentInfo = msg.info
        
        for n in self.nodes:
            if agentId == n.id:
                self.log.debug("Already known Node UUID: {}, Name: {}, Info: {}".format(agentId,agentName,agentInfo))
                return

        node = Node(msg)
        self.nodes.append(node)
        self.log.debug("New node with UUID: {}, Name: {}, Info: {}".format(agentId,agentName,agentInfo))
        self.controller.transport.subscribe_to(agentId)

        #start hello timeout timer
        node.set_timer_callback(self.remove_node_hello_timer)
        gevent.spawn(node.hello_timer)

        if node and self.newNodeCallback:
            self.newNodeCallback(node)

        dest = agentId
        cmdDesc.Clear()
        cmdDesc.type = msgs.get_msg_type(msgs.NewNodeAck)
        cmdDesc.func_name = msgs.get_msg_type(msgs.NewNodeAck)
        cmdDesc.serialization_type = msgs.CmdDesc.PROTOBUF

        msg = msgs.NewNodeAck()
        msg.status = True
        msg.controller_uuid = self.controller.uuid
        msg.agent_uuid = agentId
        msg.topics.append("ALL")

        msgContainer = [dest, cmdDesc, msg]

        time.sleep(1) # TODO: why?
        self.controller.transport.send_downlink_msg(msgContainer)
        return node


    def remove_node_hello_timer(self, node):
        reason = "HelloTimeout"
        self.log.debug("Controller removes node with UUID: {}, Reason: {}".format(node.id, reason))

        self.nodes.remove(node)

        if node and self.nodeExitCallback:
            self.nodeExitCallback(node, reason)


    def remove_node(self, msgContainer):
        topic = msgContainer[0]
        cmdDesc = msgContainer[1]
        msg = msgs.NodeExitMsg()
        msg.ParseFromString(msgContainer[2])
        agentId = str(msg.agent_uuid)
        reason = msg.reason

        node = self.get_node_by_id(agentId)

        if not node:
            return

        self.log.debug("Controller removes node with UUID: {}, Reason: {}".format(agentId, reason))

        self.nodes.remove(node)

        if node and self.nodeExitCallback:
            self.nodeExitCallback(node, reason)


    def send_hello_msg_to_node(self, nodeId):
        self.log.debug("Controller sends HelloMsg to agent")
        dest = nodeId
        cmdDesc = msgs.CmdDesc()
        cmdDesc.type = msgs.get_msg_type(msgs.HelloMsg)
        cmdDesc.func_name = msgs.get_msg_type(msgs.HelloMsg)
        cmdDesc.serialization_type = msgs.CmdDesc.PROTOBUF

        msg = msgs.HelloMsg()
        msg.uuid = str(self.controller.uuid)
        msg.timeout = self.helloTimeout
        msgContainer = [dest, cmdDesc, msg]
        self.controller.transport.send_downlink_msg(msgContainer)


    def serve_hello_msg(self, msgContainer):
        self.log.debug("Controller received HELLO MESSAGE from agent".format())
        dest = msgContainer[0]
        cmdDesc = msgContainer[1]
        msg = msgs.HelloMsg()
        msg.ParseFromString(msgContainer[2])

        self.send_hello_msg_to_node(str(msg.uuid))

        node = self.get_node_by_id(str(msg.uuid))
        node.refresh_hello_timer()