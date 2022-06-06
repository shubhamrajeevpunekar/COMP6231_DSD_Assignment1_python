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
    def __init__(self, repoID, discoveryUDPPort, erapTCPPort, repository, peerList: dict):
        super().__init__(repoID, discoveryUDPPort, erapTCPPort)
        self.peers = peerList
        self.repository = repository
        self.repositoryLock = threading.Lock()
        self.socket = None
        self.clients = []
        self.clientThreads = []
        # TODO: Save client threads for graceful shutdown

    def tcp_listener(self):
        logger.info(f"Starting ERAP TCP listener for repo id {self.repoID} on port {self.erapTCPPort}")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('', self.erapTCPPort))
        self.socket.listen(5)  # TODO: Move to config?
        logger.info("Waiting for connections")
        while True:
            (conn, address) = self.socket.accept()
            self.clients.append(address)
            t = threading.Thread(target=self.handle_client, args=(conn, address))
            logger.info(f"Connected to client: {address[0]}:{address[1]}")
            conn.send(f"OK Repository {self.repoID} ready\033[2;0H".encode("utf-8"))
            t.daemon = True
            self.clientThreads.append(t)
            t.start()
        # TODO: Thread clean up once the connection is closed?

    def handle_client(self, conn: socket.socket, address):
        line = ""
        while True:
            # data received from client
            data: bytes = conn.recv(1024)

            logger.debug(f"Received from client {address[0]}:{address[1]}: {data.decode().rstrip()}")
            # TODO: Exception to be handled, if the received data cannot be decoded as text
            # TODO: If the terminal with the telnet process is closed, the server keeps empty input indefinitely

            if len(data) == 0 or data.decode().startswith("quit"):
                conn.close()
                logger.info(f"Closing connection: {address}")

                # connection closed
                conn.close()
                break
            else:
                line += data.decode()
                if line.__contains__('\n'):
                    if line == '\n':
                        continue
                    repositoryOperation = self.parseClientRequest(line)
                    logger.debug(f"Performing {repositoryOperation} on repository {self.repoID}")
                    result = self.checkRepo(repositoryOperation)
                    conn.send(result + "\033[N".encode() + "\033[100D".encode())
                    line = ""

    def checkRepo(self, request):
        if "." in request[1]:
            key = request[1].split(".")
            repo = key[0]
            if repo in self.peers:
                if repo == self.repoID:
                    logger.debug(f"Changed {request[1]} to {key[1]} with repository {repo}")
                    request[1] = key[1]
                else:
                    for keys in self.peers.keys():
                        if repo == keys:
                            return self.sendClient(' '.join(request), self.peers[repo])
                    logger.critical(f"Error, no repository with ID {repo}")
            else:
                logger.critical(f"Error, no repository with ID {repo}")
        return self.performRepositoryOperation(request)

    def parseClientRequest(self, request):
        try:
            request = request.rstrip().split(" ")
            return request
        except UnicodeDecodeError:
            logger.critical(f"Received dirty bytes: {request}")

    def sendClient(self, request, address):
        logger.debug(f"creating tcp connection with operation {request[0]} and key {request[1]} and ip {address}")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect(address)
        for c in request:
            s.send(c.encode())
        s.send("\n".encode())
        s.send(" ".encode())
        #greeting = s.recv(1024).decode()
        data = s.recv(1024).decode()
        logger.debug(f"received data from TCP server: '{data}' as a client with ID {self.repoID}")
        s.close()
        return data

    # TODO: Add debug logs for responses
    # TODO: Updated errors to be sent back to the client
    def performRepositoryOperation(self, repositoryOperation: list):
        try:
            operation = repositoryOperation[0].lower()
            if operation == "add":
                key, value = repositoryOperation[1], int(repositoryOperation[2])
                with self.repositoryLock:
                    self.repository.add(key, value)
                logger.debug(
                    f"Added {key} -> {value}")
                return f"OK, added {key} -> {value}\n".encode()
            elif operation == "set":
                key, values = repositoryOperation[1], list([int(_) for _ in repositoryOperation[2:]])
                with self.repositoryLock:
                    self.repository.set(key, values)
                logger.debug(f"Set {key} -> {values}")
                return f"OK, set {key} -> {values}\n".encode()
            elif operation == "delete":
                with self.repositoryLock:
                    self.repository.delete(repositoryOperation[1])  # will throw KeyError if key does not exist
                logger.debug(f"Deleted key {repositoryOperation[1]}")
                return f"OK, deleted key {repositoryOperation[1]}\n".encode()
            elif operation == "keys":
                with self.repositoryLock:
                    if self.repository.keys():
                        result = "OK " + ", ".join([str(_) for _ in self.repository.keys()]) + "\n"
                    else:
                        result = "OK, empty repository\n"
                logger.debug(f'Keys: {", ".join([str(_) for _ in self.repository.keys()])}')
                return result.encode()
            elif operation == "get":
                with self.repositoryLock:
                    value = self.repository.getValue(repositoryOperation[1])
                if value is not None:
                    logger.debug(f"Key: {repositoryOperation[1]} -> {value}")
                    return ("OK " + str(value) + "\n").encode()
                else:
                    logger.debug(f"missing key {repositoryOperation[1]}")
                    return f"ERROR, missing key {repositoryOperation[1]}\n".encode()
            elif operation == "gets":
                with self.repositoryLock:
                    values = self.repository.getValues(repositoryOperation[1])
                if values is not None:
                    logger.debug(f"Key: {repositoryOperation[1]} -> {values}")
                    result = "OK " + ", ".join([str(_) for _ in values]) + "\n"
                    return result.encode()
                else:
                    return f"ERROR, missing key {repositoryOperation[1]}\n".encode()
            elif operation == "aggregate":
                key, func = repositoryOperation[1], repositoryOperation[2]
                with self.repositoryLock:
                    aggregated = self.repository.aggregate(key, func)
                if aggregated is not None:
                    logger.debug(f"Aggregate: {key} : {func} -> {aggregated}")
                    return ("OK " + str(aggregated) + "\n").encode()
                else:
                    return f"ERROR, missing key or function in {repositoryOperation[1:]}\n".encode()
            elif operation == "reset":
                with self.repositoryLock:
                    self.repository.reset()
                logger.debug("Reset repository")
                return "OK\n".encode()
            else:
                logger.critical(f"Malformed repository operation: {repositoryOperation}")
                return f"ERROR, malformed operation {repositoryOperation}\n".encode()
        except IndexError:
            logger.critical(f"Malformed repository operation: {repositoryOperation}")
            return f"ERROR, malformed operation {repositoryOperation}\n".encode()
        except ValueError:
            logger.critical(f"Type conversion failed: {repositoryOperation}")
            return f"ERROR, type conversion failed in {repositoryOperation}\n".encode()
        except KeyError:
            logger.critical(f"Non existent key provided: {repositoryOperation}")
            return f"ERROR, key missing in {repositoryOperation}\n".encode()
        except Exception:
            logger.exception(f"Error occurred for: {repositoryOperation}")
            return f"ERROR, malformed operation {repositoryOperation}\n".encode()

    def run(self):
        erap_thread = threading.Thread(target=self.tcp_listener)
        erap_thread.start()
