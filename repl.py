import logging
from discovery import peers

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(logging.DEBUG)
logger.addHandler(consoleHandler)

def repl_protocol():
    logger.info("Starting REPL")
    while (True):
        text = input()
        if text == "show":
            print(peers)
            print()

