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
    def __init__(self, repoID, discoveryUDPPort, erapTCPPort, repository, peers):
        super().__init__(repoID, discoveryUDPPort, erapTCPPort)
        self.peers = peers
        self.repository = repository
        self.repositoryLock = threading.Lock()
        self.socket = None
        self.clients = []
        self.clientThreads = []
        self.erapTCPSocket = None
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
            conn.send(f"OK Repository {self.repoID} ready\n".encode("utf-8"))
            t.daemon = True
            self.clientThreads.append(t)
            t.start()
        # TODO: Thread clean up once the connection is closed?

    def handle_client(self, conn: socket.socket, address):
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
                repositoryOperation = self.parseClientRequest(data)
                logger.debug(f"Performing {repositoryOperation} on repository {self.repoID}")
                result = self.performRepositoryOperation(repositoryOperation)
                conn.send(result)

    def parseClientRequest(self, request: bytes):
        try:
            request = request.decode("utf-8").rstrip().split(" ")
            request = [_.lower() for _ in request]
            return request
        except UnicodeDecodeError:
            logger.critical(f"Received dirty bytes: {request}")

    def performRemoteRAPfromProxy(self, repoID, repositoryOperation):
        # Will return False if the remote operation fails
        try:
            remoteRepoAddress = self.peers[repoID]  # read access, no need to lock
            # establish tcp connection with the server
            self.erapTCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.erapTCPSocket.connect(remoteRepoAddress)
            logger.info(f"Connected from {self.repoID} to remote repo: {repoID}:{remoteRepoAddress}")
            repositoryOperation = " ".join(repositoryOperation)
            self.erapTCPSocket.send(repositoryOperation.encode())
            logger.debug(f"Sent request from proxy {self.repoID} to {repoID}: {repositoryOperation}")
            result = self.erapTCPSocket.recv(2048)
            if "ready" in result.decode():
                result = self.erapTCPSocket.recv(2048)
            logger.debug(f"Received response at proxy{self.repoID} from {repoID}: {result}")
            self.erapTCPSocket.close()
            logger.info(f"Detached connection from proxy {self.repoID} to {repoID}")
            return result
        except KeyError:
            logger.critical(f"No repository with ID: {repoID}")
            return False

    def performDistributedRepositoryAggregateOperation(self, repositoryOperation):

        def error(reason):  # closure for logging errors local to this function, and returning with error
            logger.critical(f"Error in distributed repository operation: {reason}")
            return f"Error in distributed repository operation: {reason}".encode()

        # distributed aggregate functions
        # should get collections for the specified key from the given repositories first
        # if a repository does not exist, skip it
        # if one of the repositories doesn't have the key, skip it
        # if all the repositories do not have the key, return an error
        # return the result with list of repositories which have the key

        def getsFromRepository(key, repoID):
            repositoryOperation = ("gets", key)
            result = self.performRemoteRAPfromProxy(repoID, repositoryOperation)
            if result is False:
                logger.debug(f"Proxy {self.repoID} got no values for {key} from {repoID}")
                return []
            else:
                result = result.decode()
                # result will be of the format : "OK 5, 10"
                result = result.replace(",", "") # get rid of the commas between elements
                result = result.split(" ")[1:] # "Skip the OK
                result = [int(_) for _ in result]
                return result

        def getValuesFromRemoteRepos(key, repoIDs):
            values = []
            for repoID in repoIDs:
                repoValues = getsFromRepository(key, repoID)
                logger.debug(f"Recived at proxy {self.repoID} from {repoID}: {key} -> {repoValues}")
                values += repoValues

            return values

        def dmax(key, repoIDs):
            values = getValuesFromRemoteRepos(key, repoIDs)
            print(values)
        def dmin(key, repositories):
            values = getValuesFromRemoteRepos(key, repoIDs)
            print(values)
        def dsum(key, repositories):
            values = getValuesFromRemoteRepos(key, repoIDs)
            return sum(values)
        def davg(key, repositories):
            values = getValuesFromRemoteRepos(key, repoIDs)
            print(values)

        operations = {"dmax":dmax, "dmin":dmin, "dsum":dsum, "davg":davg}

        try:
            operation, key, including, repoIDs = repositoryOperation[0], repositoryOperation[1], \
                                                      repositoryOperation[2], repositoryOperation[3:]
            result = operations[operation](key, repoIDs)
            if result is None:
                error(f"Key {key} is not present in any of the repositories")
            else:
                return str(result).encode()
        except IndexError:
            error(repositoryOperation)

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
            elif "including" in repositoryOperation:
                # indicates a distribute aggregate operation
                distributedAggregate = self.performDistributedRepositoryAggregateOperation(repositoryOperation)
                logger.debug(f"Distributed aggregate: {str(distributedAggregate)}")
                return ("OK " + str(distributedAggregate) + "\n").encode()
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
