# import socket
# import sys
#
# from Dictionary import Dictionary
#
# localIP = sys.argv[1]
# localPort = int(sys.argv[2])
# localAddress = (localIP, localPort)
#
# TCPSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#
# TCPSocket.bind(localAddress)
# TCPSocket.listen(10)
#
# conn, address = TCPSocket.accept()
# conn.sendall("Welcome to Distributed Repositories\033[2;0H".encode())
#
# dic = Dictionary()
# line = ''
#
# while True:
#     data = conn.recv(2048).decode()
#     # print(data)
#     line += data
#     if line.__contains__('\n'):
#         # print(line)
#         # print('newline!')
#         if line == '\n':
#             continue
#         command = line.split(' ')
#         message = ''
#         if command[0].upper() == 'GET':
#             message = dic.get(command[1])
#         if command[0].upper() == 'GETALL':
#             message = dic.get(command[1])
#         if command[0].upper() == 'SET':
#             message = dic.set(command[1], command[2])
#         if command[0].upper() == 'DELETE':
#             message = dic.delete(command[1])
#         if command[0].upper() == 'LIST':
#             message = dic.list()
#         if command[0].upper() == 'DELETE':
#             message = dic.delete(command[1])
#         if command[0].upper() == 'ADD':
#             message = dic.add(command[1], command[2])
#         if command[0].upper() == 'MAX':
#             message = dic.max(command[1])
#         if command[0].upper() == 'MIN':
#             message = dic.min(command[1])
#         if command[0].upper() == 'AVG':
#             message = dic.avg(command[1])
#         if command[0].upper() == 'SUM':
#             message = dic.sum(command[1])
#         if command[0].upper() == 'RESET':
#             message = dic.reset()
#         # send this data to the directory: DIRECTORY.add(data)
#         if message.__contains__('Error'):
#             conn.sendall(message.encode())
#         else:
#             conn.sendall(('OK ' + message).encode())
#         line = ''
#     if not data:
#         break
#
#     # conn.sendall(data)
