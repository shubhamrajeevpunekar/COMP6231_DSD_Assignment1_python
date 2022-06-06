import logging
import socket
import statistics
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
        self.distributedOperations = {"dmax", "dmin", "dsum", "davg"}
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
                    # conn.send(result + "\033[N".encode() + "\033[100D".encode())
                    conn.send(result)
                    line = ""

    def checkRepo(self, request):
        if len(request) > 2 and "." in request[1]:
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
                    return f"Error, no repository with ID {repo}\n".encode()
            else:
                logger.critical(f"Error, no repository with ID {repo}")
                return f"Error, no repository with ID {repo}\n".encode()
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
        greeting = s.recv(1024)
        data = s.recv(1024)
        logger.debug(f"received data from TCP server: '{data.decode()}' as a client with ID {self.repoID}")
        s.close()
        return data

    # TODO: Add debug logs for responses
    # TODO: Updated errors to be sent back to the client

    def performRemoteRAPfromProxy(self, repoID, repositoryOperation):
        # Will return False if the remote operation fails
        try:
            remoteRepoAddress = self.peers[repoID]  # read access, no need to lock
            # establish tcp connection with the server
            self.erapTCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.erapTCPSocket.connect(remoteRepoAddress)
            logger.info(f"Connected from {self.repoID} to remote repo: {repoID}:{remoteRepoAddress}")
            repositoryOperation = " ".join(repositoryOperation) + "\n"
            self.erapTCPSocket.send(repositoryOperation.encode())
            logger.debug(f"Sent request from proxy {self.repoID} to {repoID}: {repositoryOperation}")
            result = self.erapTCPSocket.recv(2048)
            if "ready" in result.decode():  # will occur everytime, as each connection is freshly made
                result = self.erapTCPSocket.recv(2048)
            logger.debug(f"Received response at proxy{self.repoID} from {repoID}: {result}")
            self.erapTCPSocket.close()
            logger.info(f"Detached connection from proxy {self.repoID} to {repoID}")
            return result
        except KeyError:
            logger.critical(f"No repository with ID: {repoID}")
            return False

    def performDistributedRepositoryAggregateOperation(self, operation, key, repoIDs):
        # should get collections for the specified key from the given repositories first
        # if a repository does not exist, skip it
        # if one of the repositories doesn't have the key, skip it
        # if all the repositories do not have the key, return an error
        # return the result with list of repositories which have the key

        def getsFromRepo(key, repoID):
            if repoID == self.repoID:
                logger.debug(f"Getting values for key {key} from same repository {repoID}")
                values = self.repository.getValues(key)
                if values is None:  # key does not exist
                    values = []
                logger.debug(f"For repo {repoID}: {key} -> {values}")
                return values
            else:  # remote repository
                repositoryOperation = ("gets", key)
                result = self.performRemoteRAPfromProxy(repoID, repositoryOperation)
                if result is False:  # repoID doesn't exist, or failed to connect to repo, in which case, return []
                    logger.debug(f"Proxy {self.repoID} failed to perform operation on {repoID}")
                    return []
                else:
                    result = result.decode()
                    # result will be of the format : "OK 5, 10" or "ERROR, missing key a"
                    if "OK" in result:
                        result = result.replace(",", "")  # get rid of the commas between elements
                        result = result.split(" ")[1:]  # "Skip the OK
                        result = [int(_) for _ in result]
                    elif "ERROR" in result:
                        result = []
                    logger.debug(f"From proxy {self.repoID}, For repo {repoID}: {key} -> {result}")
                    return result

        def getValuesFromRepos(key, repoIDs):
            repos = []
            values = []
            for repoID in repoIDs:
                repoValues = getsFromRepo(key, repoID)
                if repoValues:
                    values += repoValues
                    repos.append(repoID)
            # if no repos are provided, repos, values are empty
            logger.debug(f"Valid repos having {key}: {repos}, combined list: {values}")
            return repos, values

        def dmax(key, repoIDs):
            return distributedOperation(key, repoIDs, max)

        def dmin(key, repoIDs):
            return distributedOperation(key, repoIDs, min)

        def dsum(key, repoIDs):
            return distributedOperation(key, repoIDs, sum)

        def davg(key, repoIDs):
            return distributedOperation(key, repoIDs, statistics.mean)

        def distributedOperation(key, repoIDs, op):
            repos, values = getValuesFromRepos(key, repoIDs)
            if values:
                return repos, op(values)
            else:
                return None  # the only way None is returned if key isn't found in any of the specified repos

        operations = {"dmin": dmin, "dmax": dmax, "dsum": dsum, "davg": davg}
        return operations[operation](key, repoIDs)

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
                try:
                    operation, key, including, repoIDs = repositoryOperation[0], repositoryOperation[1], \
                                                         repositoryOperation[2], repositoryOperation[3:]
                    if operation not in self.distributedOperations:
                        logger.critical(f"ERROR, incorrect distributed operation: {operation}")
                        return f"ERROR, incorrect distributed operation: {operation}\n".encode()
                except IndexError:
                    logger.critical("Malformed distributed repository operation")
                    return f"ERROR, malformed operation {repositoryOperation}\n".encode()

                distributedAggregatedResult = self.performDistributedRepositoryAggregateOperation(operation, key,
                                                                                                  repoIDs)
                if distributedAggregatedResult is not None:
                    repos, distributedAggregate = distributedAggregatedResult
                    logger.debug(f"Distributed aggregate: {repos}, {distributedAggregate}")
                    return (f"OK from [{', '.join(repos)}]: aggregated value: {distributedAggregate}\n").encode()
                else:
                    # reponse is None, which means the given key is not present in any of the valid repos
                    return "ERROR: key missing from valid repos\n".encode()
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
