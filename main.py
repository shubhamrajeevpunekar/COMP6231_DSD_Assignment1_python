# https://stackoverflow.com/questions/64066634/sending-broadcast-in-python
import logging
import sys

from config import PEER_DISCOVERY_UDP_PORT
from peer import Peer

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(logging.DEBUG)
logger.addHandler(consoleHandler)

repo_id = sys.argv[1]
erap_tcp_port = int(sys.argv[2])

peer = Peer(repo_id, PEER_DISCOVERY_UDP_PORT, erap_tcp_port)
logger.info(f"Running : {peer}")
peer.run()

