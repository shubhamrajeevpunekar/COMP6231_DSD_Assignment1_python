import socket
import sys

localIP = sys.argv[1]
localPort = int(sys.argv[2])
localAddress = (localIP, localPort)

TCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

TCPSocket.bind(localAddress)
TCPSocket.listen(65536)

conn, address = TCPSocket.accept()
str = ''
while(True):
    data = conn.recv(1024).decode()
    #print(data)
    str += data
    if str.__contains__('\n'):
        print(str)
        print('newline!')
        #conn.sendall(str.encode())
        # send this data to the directory: DIRECTORY.add(data)
        str = ''
    if not data:
        break

    #conn.sendall(data)

