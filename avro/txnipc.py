# -*- coding: UTF-8 -*-
#-*- author: Liuzh -*-
from twisted.internet import reactor
from twisted.internet.protocol import Protocol


class TxNipcProtocol(Protocol):
    def connectionMade(self):
        self.sendData()

    def dataReceived(self, data):
        self.sendData()