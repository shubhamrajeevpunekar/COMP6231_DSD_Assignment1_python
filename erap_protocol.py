import logging
import socket
import threading

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)

def handle_client(conn:socket.socket, address):
    while True:
        # data received from client
        data:bytes = conn.recv(1024)

        logger.info(f"ERAP: CLIENT: Received from client {address[0]}:{address[1]}: {data.decode()}")
        # TODO: Exception to be handled, if the received data cannot be decoded as text
        # TODO: If the terminal with the telnet process is closed, the server keeps empty input indefinitely

        if len(data) == 0 or data.decode().startswith("quit"):
            conn.close()
            print("ERAP: CLIENT: Closing connection: ", address)

            # connection closed
            conn.close()
            break
        else:
            # send back reversed string to client
            conn.send(data)

def erap(repo_id, erap_tcp_port):
    logger.info(f"ERAP: Starting TCP server for repo id {repo_id} on port {erap_tcp_port}")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('', erap_tcp_port))
    s.listen(5)
    logger.info(f"ERAP: Waiting for connections")
    while True:
        (conn, address) = s.accept()
        t = threading.Thread(target=handle_client, args=(conn, address))
        logger.warning(f"ERAP: Connected to client: {address[0]}:{address[1]}")
        t.daemon = True
        t.start()
    # TODO: Thread clean up once the connection is closed?