# https://stackoverflow.com/questions/64066634/sending-broadcast-in-python
import sys
import socket
import logging
import threading
import time

import erap_protocol
from peer_discovery_protocol import peer_discovery_loop, peer_advertising, peers
from config import PEER_DISCOVERY_UDP_PORT
# Each peer requires:
# 1. Repository ID
# 2. Data dictionary for the repository
# 3. Dictionary for repository ids and sockets
# 3. TCP socket for Extended Repository Access Protocol (ERAP)
# 4. UDP socket for receiving broadcasted peer discovery messages

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)

repository = dict()
repo_id = int(sys.argv[1])
erap_tcp_port = int(sys.argv[2])

logger.info(f"Starting peer with repo ID: {repo_id} on peer protocol port: {PEER_DISCOVERY_UDP_PORT}")

# Create a datagram socket and bind ip, port
discovery_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
discovery_socket.bind(('', PEER_DISCOVERY_UDP_PORT))

peer_discovery_listener_thread = threading.Thread(target=peer_discovery_loop, args=[discovery_socket, repo_id, erap_tcp_port])
peer_discovery_listener_thread.start()
logger.info("MAIN: Peer discovery running")

peer_advertising_thread = threading.Thread(target=peer_advertising, args=[repo_id, erap_tcp_port])
peer_advertising_thread.start()
logger.info("MAIN: Peer advertising running")

erap_thread = threading.Thread(target=erap_protocol.erap, args=[repo_id, erap_tcp_port])
erap_thread.start()
logger.info("MAIN: ERAP tcp server running")

def testFunction():
    while(True):
        text = input()
        if text == "show":
            print(peers)
thread = threading.Thread(target=testFunction)
thread.start()


