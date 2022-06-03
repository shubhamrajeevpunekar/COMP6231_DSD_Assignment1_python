from discovery import DiscoveryProtocol
from protocol import Protocol


class Peer(Protocol):
    def __init__(self, repoID, discoveryUDPPort, erapTCPPort):
        super().__init__(repoID, discoveryUDPPort, erapTCPPort)
        self.peers = dict()
        self.repo = dict()  # TODO: to be replaced with Repository

        self.discoveryProtocol = DiscoveryProtocol(self.repoID, self.discoveryUDPPort, self.erapTCPPort, self.peers)
        # self.erapProtocol = ERAPProtocol()
        # self.replProtocol = REPLProtocl()

    def __str__(self):
        return f"Peer with repoID: {self.repoID}, discoveryUDPPort: {self.discoveryUDPPort}, erapTCPPort: {self.erapTCPPort}"

    def run(self):
        self.discoveryProtocol.run()
        # self.erapProtocol.start()
        # self.replProtocol.start()
