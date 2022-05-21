# https://stackoverflow.com/questions/64066634/sending-broadcast-in-python
import sys
import socket
import logging
import threading
import time

# Each peer requires:
# 1. Repository ID
# 2. Data dictionary for the repository
# 3. Dictionary for repository ids and sockets
# 3. TCP socket for Extended Repository Access Protocol (ERAP)
# 4. UDP socket for receiving broadcasted peer discovery messages

logging.basicConfig(level=logging.INFO)

repository = dict()
peers = dict()
repo_id = int(sys.argv[1])
peer_discovery_udp_port = int(sys.argv[2])
# erap_tcp_port = sys.argv[3]

logging.info(f"Starting peer with repo ID: {repo_id} on peer protocol port: {peer_discovery_udp_port}")
# When the peer starts, it broadcasts it's repository ID, TCP socket
bufferSize = 1024
msgFromServer = "Hello UDP Client"
bytesToSend = str.encode(msgFromServer)

# Create a datagram socket and bind ip, port
peer_protocol_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
peer_protocol_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
peer_protocol_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
peer_protocol_socket.bind(('', peer_discovery_udp_port))

def peer_discovery_loop():
    logging.info("DISCOVERY: Starting peer discovery")
    while(True):
        bytesAddressPair = peer_protocol_socket.recvfrom(bufferSize)
        message = bytesAddressPair[0]
        address = bytesAddressPair[1]
        clientMsg = "DISCOVERY: Message from Client:{} @ Address: {}".format(message, address)
        print(clientMsg)

peer_discovery_listener_thread = threading.Thread(target=peer_discovery_loop)
peer_discovery_listener_thread.start()
logging.info("MAIN: Peer discovery running")

def peer_advertising_loop():
    # broadcast repo id every n seconds
    n = 5
    logging.info(f"ADVERTISING: Starting peer advertising, broadcasting every {n} seconds")
    counter = 0
    while(True):
        logging.info(f"ADVERTISING: {counter}")
        counter += 1
        peer_protocol_socket.sendto(bytes(f"Repo: {repo_id}", "utf-8"), ("255.255.255.255", peer_discovery_udp_port))
        time.sleep(n)

peer_discovery_advertising_thread = threading.Thread(target=peer_advertising_loop)
peer_discovery_advertising_thread.start()
logging.info("MAIN: Peer advertising running")


