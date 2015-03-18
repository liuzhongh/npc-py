# -*- coding: UTF-8 -*-
#-*- author: Liuzh -*-
import logging
from avro.npc import NettyClient

fmt = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(message)s'

logging.basicConfig(level=logging.INFO,
                format=fmt)

from avro import protocol, npc
import avro.nipc as nipc

if __name__ == '__main__':
    SERVER_HOST = 'localhost'
    SERVER_PORT = 8080
    PROTOCOL = protocol.Parse(open("/media/E/work/javaProjects/npc/src/test/code/core/npc/test/Service1.avpr").read())
    params = dict()

    params['str'] = 'is python'

    for i in range(0, 1):
        try:
            client = nipc.SocketTransceiver(SERVER_HOST, SERVER_PORT)
            requestor = nipc.Requestor(PROTOCOL, client)
            print(requestor.Request('helloWorld', 'getNoArg'))

            # client = nipc.SocketTransceiver(SERVER_HOST, SERVER_PORT)
            # requestor = nipc.Requestor(PROTOCOL, client)
            print(requestor.Request('helloWorld2', 'getVoid', params))

            nettyClient = NettyClient(SERVER_HOST, SERVER_PORT)
            req = npc.ClientRequestor()
            result = req.getClient(nettyClient, 'helloWorld', 'getNoArg', PROTOCOL, params)

            print(result)

            result = req.getClient(nettyClient, 'helloWorld2', 'getVoid', PROTOCOL, params)

            print(result)
        except Exception as e:
            raise e
        finally:
            if client is not None:
                client.Close()
            if 'nettyClient' in locals() and nettyClient is not None:
                nettyClient.close()
