from client import Client
from csv import DictReader
import os
from _thread import *
import json
import sys
import time


HASH_SIZE = 353 # Size to initialize the local hash table to
BUFFER_SIZE = 4096 # Max bytes to take in
FILE_PATH = "StatsCountry.csv"


def end_script(message):
    if message:
        print(message)
    sys.exit()


def listen(client):
    '''
        Listen function that will listen to all responses from the server connected to client
    '''
    while True:
        data = client.client_to_server.socket.recv(BUFFER_SIZE)
        data_loaded = data.decode('utf-8')

        if data_loaded:
            try:
                data_loaded = json.loads(data_loaded)
            except:
                print("error with json.load")
                return
        # print(data_loaded)
        
        if data_loaded and data_loaded['res'] == 'SUCCESS':
            print("client: received SUCCESS response from server")
            if data_loaded['data']:
                print(f"\nclient: received data {data_loaded['data']} from server on IP address {client.server_addr[0]}\n")
            
            if data_loaded['type'] == 'DHT':
                client.set_data(data_loaded['data'], index=-1)
                client.connect_all_nodes()
                client.setup_all_local_dht()
                success_string = bytes(f'dht-complete {client.username}', 'utf-8')
                try:
                    client.client_to_server.socket.sendto(success_string, client.server_addr)
                except:
                    print("client: sendall() error sending success string")
                    return
            elif data_loaded['type'] == 'query-response':
                query_long_name = input("Enter query followed by the Long Name to query: ")
                client.query = ' '.join(query_long_name.split()[1:])
                # print(f"Received query of {client.query}")
                first_ip = data_loaded['data']['ip']
                first_port = int(data_loaded['data']['query'])
                client.connect_query_nodes(origin=client.query_addr, ip=first_ip, port=first_port)
                # print(response)
            elif data_loaded['type'] == 'deregister':
                client.terminate = True
                end_script(f"{data_loaded['data']}\nTerminating client application.")
            elif data_loaded['type'] == 'leave-response':
                client.leaving_user = True
                client.send_port.send_response(addr=client.next_node_addr, res='SUCCESS', type='leaving-teardown')
            elif data_loaded['type'] == 'leave-teardown-complete':
                # Now we need to iterate through the nodes until we get to the user leaving
                if data_loaded['data'] == client.username:
                    # Leader is the user supposed to leave
                    client.leave_dht()
        else:
            print(data_loaded)


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
    

    print("Starting thread to listen to server\n")
    start_new_thread(listen, (client, ))
    
    while True:
        time.sleep(0.5)
        if client.terminate:
            sys.exit()
        echo_string = input("\nEnter command for the server: ")

        if echo_string:
            # print(f"\nClient: reads string ``{echo_string}''\n")
            echo_string = bytes(echo_string, 'utf-8')
            try:
                client.client_to_server.socket.sendto(echo_string, client.server_addr)
                
            except Exception as error:
                print(error)
                print("client: sendall() error")
        else:
            print("client: error reading string to forward to server\n")
                


if __name__ == "__main__":
    main(sys.argv)