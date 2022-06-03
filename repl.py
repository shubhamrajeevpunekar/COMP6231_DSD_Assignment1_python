import logging
from protocol import Protocol

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(logging.DEBUG)
logger.addHandler(consoleHandler)

class REPLProtocol(Protocol):
    def __init__(self, repoID, discoveryUDPPort, erapTCPPort, peer):
        super(REPLProtocol, self).__init__(repoID, discoveryUDPPort, erapTCPPort)
        self.peer = peer

    def run(self):
        logger.info("Starting REPL")

        while (True):
            text = input()
            if text == "show":
                print(self.peer.peers)
                print()
        # Add commands for the following
        # TODO: Show clients
        # TODO: graceful shutdown
        # TODO: re-advertise
