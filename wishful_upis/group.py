


class Group():

    def __init__(self):
        self.log = logging.getLogger("{module}.{name}".format(
            module=self.__class__.__module__, name=self.__class__.__name__))

    def send(self, upi_object):
        self.log.debug("should send zmq msq to the whole group")
        self.pub.send_multipart(
            [self.topic,
             upi_object.type,
             upi_object.serialize()
             ])
        return None