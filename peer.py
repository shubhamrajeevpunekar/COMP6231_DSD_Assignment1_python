# https://stackoverflow.com/questions/64066634/sending-broadcast-in-python
import sys
import socket
import threading
import logging

import erap
from discovery import peer_discovery_loop, peer_advertising, peers
from config import PEER_DISCOVERY_UDP_PORT

# Each peer requires:
# 1. Repository ID
# 2. Data dictionary for the repository
# 3. Dictionary for repository ids and sockets
# 3. TCP socket for Extended Repository Access Protocol (ERAP)
# 4. UDP socket for receiving broadcasted peer discovery messages

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(logging.DEBUG)
logger.addHandler(consoleHandler)

repository = dict()
repo_id = int(sys.argv[1])
erap_tcp_port = int(sys.argv[2])

logger.info(f"Starting peer with repo ID: {repo_id}, peer discovery: {PEER_DISCOVERY_UDP_PORT}, erap: {erap_tcp_port}")

# Create a datagram socket and bind ip, port
discovery_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
discovery_socket.bind(('', PEER_DISCOVERY_UDP_PORT))  # Specifying '' allows it to be bound to the network ip

peer_discovery_listener_thread = threading.Thread(target=peer_discovery_loop,
                                                  args=[discovery_socket, repo_id, erap_tcp_port])
peer_discovery_listener_thread.start()

peer_advertising_thread = threading.Thread(target=peer_advertising, args=[repo_id, erap_tcp_port])
peer_advertising_thread.start()

erap_thread = threading.Thread(target=erap.tcp_listener, args=[repo_id, erap_tcp_port])
erap_thread.start()


def testFunction():
    while (True):
        text = input()
        if text == "show":
            print(peers)


thread = threading.Thread(target=testFunction)
thread.start()
