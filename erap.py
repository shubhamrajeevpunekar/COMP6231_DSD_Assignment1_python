import logging
import socket
import threading

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(logging.DEBUG)
logger.addHandler(consoleHandler)

def handle_client(conn:socket.socket, address):
    while True:
        # data received from client
        data:bytes = conn.recv(1024)

        logger.debug(f"Received from client {address[0]}:{address[1]}: {data.decode()}")
        # TODO: Exception to be handled, if the received data cannot be decoded as text
        # TODO: If the terminal with the telnet process is closed, the server keeps empty input indefinitely

        if len(data) == 0 or data.decode().startswith("quit"):
            conn.close()
            logger.info(f"Closing connection: {address}")

            # connection closed
            conn.close()
            break
        else:
            # send back reversed string to client
            conn.send(data)

def tcp_listener(repo_id, erap_tcp_port):
    logger.info(f"Starting erap TCP listener for repo id {repo_id} on port {erap_tcp_port}")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', erap_tcp_port))
    s.listen(5)
    logger.info("Waiting for connections")
    while True:
        (conn, address) = s.accept()
        t = threading.Thread(target=handle_client, args=(conn, address))
        logger.info(f"Connected to client: {address[0]}:{address[1]}")
        t.daemon = True
        t.start()
    # TODO: Thread clean up once the connection is closed?