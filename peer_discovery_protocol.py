import logging
import socket
import time
from config import PEER_DISCOVERY_UDP_PORT
from threading import Lock

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)

peers = dict()
peer_lock = Lock()
bufferSize = 1024

def peer_discovery_loop(discovery_socket: socket.socket, repo_id):
    logger.info("DISCOVERY: Starting peer discovery")
    while(True):

        bytesAddressPair = discovery_socket.recvfrom(bufferSize)
        message = bytesAddressPair[0]
        address = bytesAddressPair[1]

        peer_message = "DISCOVERY: Message from peer:{} @ Address: {}".format(message, address)
        logger.info(peer_message)

        with peer_lock:
            advertised_repo_id = int(message)
            peers[advertised_repo_id] = "ADDRESS:PORT"

        # Respond by sending the details of this peer, so that they can be saved by new peer on the network
        # If message is from self i.e. same repo id, skip response
        if (advertised_repo_id != repo_id):
            logger.info(f"DISCOVERY: Responding back to {address} with {repo_id}")
            discovery_socket.sendto(bytes(f"{repo_id}", "utf-8"), address)

def peer_advertising(repo_id):
    logger.info(f"ADVERTISING: broadcasting Repo ID: {repo_id} TCP Port: TODO")

    advertising_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    advertising_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    advertising_socket.sendto(bytes(f"{repo_id}", "utf-8"), ("255.255.255.255", PEER_DISCOVERY_UDP_PORT))

    while(True):
        bytesAddressPair = advertising_socket.recvfrom(bufferSize)
        message = bytesAddressPair[0]
        address = bytesAddressPair[1]

        peer_message = "ADVERTISING: Message from peer:{} @ Address: {}".format(message, address)
        logger.info(peer_message)

        with peer_lock:
            discovered_repo_id = int(message)
            peers[discovered_repo_id] = "ADDRESS:PORT"



