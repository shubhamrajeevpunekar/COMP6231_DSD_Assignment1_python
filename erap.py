import logging
import socket
import threading

from protocol import Protocol

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(logging.DEBUG)
logger.addHandler(consoleHandler)


class ERAPProtocol(Protocol):
    def __init__(self, repoID, discoveryUDPPort, erapTCPPort):
        super().__init__(repoID, discoveryUDPPort, erapTCPPort)
        self.socket = None
        self.clients = []
        self.clientThreads = []
        # TODO: Save client threads for graceful shutdown

    def tcp_listener(self):
        logger.info(f"Starting ERAP TCP listener for repo id {self.repoID} on port {self.erapTCPPort}")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('', self.erapTCPPort))
        self.socket.listen(5) # TODO: Move to config?
        logger.info("Waiting for connections")
        while True:
            (conn, address) = self.socket.accept()
            self.clients.append(address)
            t = threading.Thread(target=self.handle_client, args=(conn, address))
            logger.info(f"Connected to client: {address[0]}:{address[1]}")
            t.daemon = True
            self.clientThreads.append(t)
            t.start()
        # TODO: Thread clean up once the connection is closed?

    def handle_client(self, conn: socket.socket, address):
        while True:
            # data received from client
            data: bytes = conn.recv(1024)

            logger.debug(f"Received from client {address[0]}:{address[1]}: {data.decode()}")
            # TODO: Exception to be handled, if the received data cannot be decoded as text
            # TODO: If the terminal with the telnet process is closed, the server keeps empty input indefinitely

            if len(data) == 0 or data.decode().startswith("quit"):
                conn.close()
                logger.info(f"Closing connection: {address}")

                # connection closed
                conn.close()
                break
            else:
                # send back reversed string to client
                conn.send(data)

    def run(self):
        erap_thread = threading.Thread(target=self.tcp_listener)
        erap_thread.start()
