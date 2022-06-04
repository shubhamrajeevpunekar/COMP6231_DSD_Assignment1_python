from discovery import DiscoveryProtocol
from erap import ERAPProtocol
from protocol import Protocol
from repl import REPLProtocol
from repository import Repository


# TODO: Graceful server shutdown, on a "shutdown" repl command?
class Peer(Protocol):
    def __init__(self, repoID, discoveryUDPPort, erapTCPPort):
        super().__init__(repoID, discoveryUDPPort, erapTCPPort)
        self.peers = dict()
        self.repository = Repository()

        self.discoveryProtocol = DiscoveryProtocol(self.repoID, self.discoveryUDPPort, self.erapTCPPort, self.peers)
        self.erapProtocol = ERAPProtocol(self.repoID, self.discoveryUDPPort, self.erapTCPPort, self.repository)
        self.replProtocol = REPLProtocol(self.repoID, self.discoveryUDPPort, self.erapTCPPort, self)

    def __str__(self):
        return f"Peer with repoID: {self.repoID}, discoveryUDPPort: {self.discoveryUDPPort}, erapTCPPort: {self.erapTCPPort}"

    def run(self):
        self.discoveryProtocol.run()
        self.erapProtocol.run()
        self.replProtocol.run()
