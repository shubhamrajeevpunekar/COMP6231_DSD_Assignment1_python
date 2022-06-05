import logging
import socket
from threading import Lock, Thread

from config import PEER_DISCOVERY_UDP_PORT
from protocol import Protocol

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(logging.DEBUG)
logger.addHandler(consoleHandler)


class AdvertisingMessage:
    def __init__(self, repo_id, erap_tcp_port):
        self.repo_id = repo_id
        self.erap_tcp_port = erap_tcp_port

    def __str__(self):
        return f"{self.repo_id}:{self.erap_tcp_port}"


class DiscoveryProtocol(Protocol):
    def __init__(self, repoID, discoveryUDPPort, erapTCPPort, peers):
        super(DiscoveryProtocol, self).__init__(repoID, discoveryUDPPort, erapTCPPort)

        self.peers = peers
        self.peerLock = Lock()
        self.bufferSize = 1024

    def discovery(self, discovery_socket: socket.socket):
        logger.info("Starting peer discovery")
        while (True):

            bytesAddressPair = discovery_socket.recvfrom(self.bufferSize)
            message = bytesAddressPair[0].decode('utf-8')
            address = bytesAddressPair[1]

            peer_message = "Advertising message received from peer:{} @ Address: {}".format(message, address)
            logger.info(peer_message)

            with self.peerLock:
                advertised_repo_id = message.split(":")[0]
                advertised_port = int(message.split(":")[1])
                self.peers[advertised_repo_id] = (address[0], advertised_port)
                logger.info(
                    f"Saved new repository with repo id: {advertised_repo_id} on erap tcp port: {advertised_port}")

            # Respond by sending the details of this peer, so that they can be saved by new peer on the network
            # If message is from self i.e. same repo id, skip response
            if (advertised_repo_id != self.repoID):
                responseMessage = AdvertisingMessage(self.repoID, self.erapTCPPort)
                logger.info(f"Responding back to {address} with {responseMessage}")
                discovery_socket.sendto(bytes(f"{responseMessage}", "utf-8"), address)

    def advertising(self):
        logger.info(f"Broadcasting advertising message for Repo ID: {self.repoID} erap port: {self.erapTCPPort}")

        advertising_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        advertising_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        advertisingMessage = AdvertisingMessage(self.repoID, self.erapTCPPort)
        advertising_socket.sendto(bytes(f"{advertisingMessage}", "utf-8"), ("255.255.255.255", PEER_DISCOVERY_UDP_PORT))

        while (True):
            bytesAddressPair = advertising_socket.recvfrom(self.bufferSize)
            message = bytesAddressPair[0].decode('utf-8')
            address = bytesAddressPair[1]

            peer_message = "Advertising message received from peer:{} @ Address: {}".format(message, address)
            logger.info(peer_message)

            with self.peerLock:
                advertised_repo_id = message.split(":")[0]
                advertised_port = int(message.split(":")[1])
                self.peers[advertised_repo_id] = (address[0], advertised_port)
                logger.debug(
                    f"Saved new repository with repo id: {advertised_repo_id} on erap tcp port: {advertised_port}")

    def run(self):
        # Create a datagram socket and bind ip, port
        discovery_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        discovery_socket.bind(('', PEER_DISCOVERY_UDP_PORT))  # Specifying '' allows it to be bound to the network ip

        discovery_thread = Thread(target=self.discovery,
                                  args=[discovery_socket])
        discovery_thread.start()

        advertising_thread = Thread(target=self.advertising)
        advertising_thread.start()
