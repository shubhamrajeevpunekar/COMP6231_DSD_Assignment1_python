# import argparse
# import json
# import socket
# import threading
#
# def handle_client(client_list, conn, address):
#     name = conn.recv(1024)
#     entry = dict(zip(['name', 'address', 'port'], [name, address[0], address[1]]))
#     client_list[name] = entry
#     conn.sendall(json.dumps(client_list))
#     conn.shutdown(socket.SHUT_RDWR)
#     conn.close()
#
# def server(client_list):
#     print("Starting server...")
#     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#     s.bind(('127.0.0.1', 5000))
#     s.listen(5)
#     while True:
#         (conn, address) = s.accept()
#         t = threading.Thread(target=handle_client, args=(client_list, conn, address))
#         t.daemon = True
#         t.start()
#
# def client(name):
#     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     s.connect(('127.0.0.1', 5000))
#     s.send(name)
#     data = s.recv(1024)
#     result = json.loads(data)
#     print(json.dumps(result, indent=4))
#
# def parse_arguments():
#     parser = argparse.ArgumentParser()
#     parser.add_argument('-c', dest='client', action='store_true')
#     parser.add_argument('-n', dest='name', type=str, default='name')
#     result = parser.parse_args()
#     return result
#
# def main():
#     client_list = dict()
#     args = parse_arguments()
#     if args.client:
#         client(args.name)
#     else:
#         try:
#             server(client_list)
#         except KeyboardInterrupt:
#             print("Keyboard interrupt")
#
# if __name__ == '__main__':
#     main()