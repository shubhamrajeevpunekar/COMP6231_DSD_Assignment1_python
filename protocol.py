import abc

class Protocol(abc.ABC):
    def __init__(self, repoID, discoveryUDPPort, erapTCPPort):
        self.repoID = repoID
        self.discoveryUDPPort = discoveryUDPPort
        self.erapTCPPort = erapTCPPort

    @abc.abstractmethod
    def run(self):
        pass