import logging
import time
import sys
import zmq.green as zmq
import wishful_framework as msgs
try:
   import cPickle as pickle
except:
   import pickle

__author__ = "Piotr Gawlowicz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "gawlowicz@tkn.tu-berlin.de"


class TransportChannel(object):
    def __init__(self, uplink, downlink):
        self.log = logging.getLogger("{module}.{name}".format(
            module=self.__class__.__module__, name=self.__class__.__name__))
        
        self.context = zmq.Context()
        self.poller = zmq.Poller()

        self.ul_socket = self.context.socket(zmq.SUB) # one SUB socket for uplink communication over topics
        self.ul_socket.setsockopt(zmq.SUBSCRIBE,  "NEW_NODE")
        self.ul_socket.setsockopt(zmq.SUBSCRIBE,  "NODE_EXIT")
        self.ul_socket.setsockopt(zmq.SUBSCRIBE,  "RESPONSE")
        self.ul_socket.bind(uplink)

        self.dl_socket = self.context.socket(zmq.PUB) # one PUB socket for downlink communication over topics
        self.dl_socket.bind(downlink)

        #register UL socket in poller
        self.poller.register(self.ul_socket, zmq.POLLIN)

        self.recv_callback = None

    def subscribe_to(self, topic):
        self.ul_socket.setsockopt(zmq.SUBSCRIBE, topic)
 
    def set_recv_callback(self, callback):
        self.recv_callback = callback

    def send_downlink_msg(self, msgContainer):
        self.dl_socket.send_multipart(msgContainer)

    def start(self):
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
            if cmdDesc.serialization_type == msgs.CmdDesc.PICKLE:
                msg = pickle.loads(msg)

            msgContainer[0] = group
            msgContainer[1] = cmdDesc
            msgContainer[2] = msg
            self.recv_callback(msgContainer)


    def stop(self):
        self.ul_socket.setsockopt(zmq.LINGER, 0)
        self.dl_socket.setsockopt(zmq.LINGER, 0)
        self.ul_socket.close()
        self.dl_socket.close()
        self.context.term() 