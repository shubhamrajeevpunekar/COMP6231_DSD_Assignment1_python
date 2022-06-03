import logging
import socket
import time
import logging
from config import PEER_DISCOVERY_UDP_PORT
from threading import Lock

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(logging.DEBUG)
logger.addHandler(consoleHandler)

peers = dict()
peer_lock = Lock()
bufferSize = 1024


class AdvertisingMessage:
    def __init__(self, repo_id, erap_tcp_port):
        self.repo_id = repo_id
        self.erap_tcp_port = erap_tcp_port

    def __str__(self):
        return f"{self.repo_id}:{self.erap_tcp_port}"

def peer_discovery_loop(discovery_socket: socket.socket, repo_id, erap_tcp_port):
    logger.info("Starting peer discovery")
    while (True):

        bytesAddressPair = discovery_socket.recvfrom(bufferSize)
        message = bytesAddressPair[0].decode('utf-8')
        address = bytesAddressPair[1]

        peer_message = "Advertising message received from peer:{} @ Address: {}".format(message, address)
        logger.info(peer_message)

        with peer_lock:
            advertised_repo_id = int(message.split(":")[0])
            advertised_port = int(message.split(":")[1])
            peers[advertised_repo_id] = (address[0], advertised_port)
            logger.info(f"Saved new repository with repo id: {advertised_repo_id} on erap tcp port: {advertised_port}")

        # Respond by sending the details of this peer, so that they can be saved by new peer on the network
        # If message is from self i.e. same repo id, skip response
        if (advertised_repo_id != repo_id):
            responseMessage = AdvertisingMessage(repo_id, erap_tcp_port)
            logger.info(f"Responding back to {address} with {responseMessage}")
            discovery_socket.sendto(bytes(f"{responseMessage}", "utf-8"), address)


def peer_advertising(repo_id, erap_tcp_port):
    logger.info(f"Broadcasting advertising message for Repo ID: {repo_id} erap port: {erap_tcp_port}")

    advertising_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    advertising_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    advertisingMessage = AdvertisingMessage(repo_id, erap_tcp_port)
    advertising_socket.sendto(bytes(f"{advertisingMessage}", "utf-8"), ("255.255.255.255", PEER_DISCOVERY_UDP_PORT))

    while (True):
        bytesAddressPair = advertising_socket.recvfrom(bufferSize)
        message = bytesAddressPair[0].decode('utf-8')
        address = bytesAddressPair[1]

        peer_message = "Advertising message received from peer:{} @ Address: {}".format(message, address)
        logger.info(peer_message)

        with peer_lock:
            advertised_repo_id = int(message.split(":")[0])
            advertised_port = int(message.split(":")[1])
            peers[advertised_repo_id] = (address[0], advertised_port)
            logger.debug(f"Saved new repository with repo id: {advertised_repo_id} on erap tcp port: {advertised_port}")