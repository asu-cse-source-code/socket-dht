from Client import Client
from _thread import *
import json
import sys
import time


HASH_SIZE = 353 # Size to initialize the local hash table to
BUFFER_SIZE = 4096 # Max bytes to take in
FILE_PATH = "StatsCountry.csv"


def main(args):
    if not (len(args) == 7):
        print(f"Usage: {args[0]} <Server IP address> <Server Port> <Client IP address> <Client Accept Port> <Client Query Port> <Client Send Port>\n")
        quit()
    
    serv_IP = args[1]  # First arg: server IP address (dotted decimal)
    echo_serv_port = int(args[2])  # Second arg: Use given port
    client_IP = args[3]
    client_port = int(args[4])
    query_ip = client_IP
    query_port = int(args[5])
    right_ip = client_IP
    right_port = int(args[6])

    client = Client(serv_IP, echo_serv_port, client_IP, client_port, query_ip, query_port, right_ip, right_port, HASH_SIZE, BUFFER_SIZE, FILE_PATH)

    # Start the client server
    print('Starting client topology socket\n')
    start_new_thread(client.initialize_acceptance_port, ())

    # Start the client query server
    print("Starting client query socket\n")
    start_new_thread(client.client_query_socket, ())
        

    # Start socket for connection to main server
    print(f"client: Arguments passed: server IP {client.server_addr}\n")
    

    # print("Starting thread to listen to server\n")
    # start_new_thread(listen, (client, ))
    
    while True:
        time.sleep(0.2)
        user_input = input("\nEnter command for the server: ")
        data_list = user_input.split()
        command = data_list[0]

        if command == 'check-node':
            client.output_node_info()
        elif user_input:
            # print(f"\nClient: reads string ``{echo_string}''\n")
            user_input = bytes(user_input, 'utf-8')
            try:
                client.client_to_server.socket.sendto(user_input, client.server_addr)
                client.listen()
            except Exception as error:
                print(error)
                print("client: sendall() error")
        else:
            print("client: error reading string to forward to server\n")
                


if __name__ == "__main__":
    main(sys.argv)