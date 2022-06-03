# https://stackoverflow.com/questions/64066634/sending-broadcast-in-python
import logging
import sys

from config import PEER_DISCOVERY_UDP_PORT
from peer import Peer

# Each peer requires:
# 1. Repository ID
# 2. Data dictionary for the repository
# 3. Dictionary for repository ids and sockets
# 3. TCP socket for Extended Repository Access Protocol (ERAP)
# 4. UDP socket for receiving broadcasted peer discovery messages
# from repl import repl_protocol

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(logging.DEBUG)
logger.addHandler(consoleHandler)

repository = dict()
repo_id = int(sys.argv[1])
erap_tcp_port = int(sys.argv[2])

# repl_thread = threading.Thread(target=repl_protocol)
# repl_thread.start()

peer = Peer(repo_id, PEER_DISCOVERY_UDP_PORT, erap_tcp_port)
logger.info(f"Running : {peer}")
peer.run()

