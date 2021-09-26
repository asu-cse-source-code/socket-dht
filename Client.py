from Server import UDPServer
from csv import DictReader
import os
from _thread import *
import json
import socket
import sys
import time


HASH_SIZE = 353 # Size to initialize the local hash table to
BUFFER_SIZE = 4096 # Max bytes to take in

class Client:
    '''The Client class that has a single instance for each client running'''
    def __init__(self, serv_ip, serv_port, client_ip, client_port, query_ip, query_port):
        self.server_addr = (serv_ip, serv_port)
        self.client_addr = (client_ip, client_port)
        self.query_addr = (query_ip, query_port)
        self.next_node_addr = None
        self.next_node_query_addr = None
        self.record = None
        self.query = None
        self.began_query = False
        # self.client_conn = None
        self.id = None
        self.username = None
        self.n = None
        self.local_hash_table = self.setup_local_hash_table()
        self.user_dht = None

    def setup_local_hash_table():
        '''Simple function used to initialize the local hash table to the constant size'''
        return [ [] for _ in range(HASH_SIZE) ]


def die_with_error(error_message):
    '''Function to kill the program and ouput the error message'''
    sys.exit(error_message)

def hash_pos(record):
    '''Calculate the pos variable with this hash function'''
    ascii_sum = 0
    for letter in record['Long Name']:
        ascii_sum += ord(letter)
    
    return ascii_sum % 353

def check_record(client, record):
    '''
        Check given record and see if it is stored on the local hash table
        If it is not set the client.record value to the record which will
        trigger the query socket and send the query command to next node
    '''
    pos = hash_pos(record)
    id = pos % client.n
    if id == client.id:
        # This is the desired location for record!
        client.local_hash_table[pos].append(record)
    else:
        # This is not the desired location for the record
        # print(f'The desired location is on id: {id}')
        loops = 1
        # While loop that iterates until the current record is sent to next node
        while client.record:
            time.sleep(1)
            # print(f'\nawaiting record to send: {loops}')
            if loops > 4:
                # print("\nTimeout occurred!\n\n")
                die_with_error("The nodes lost connection with eachother while trying to transfer the records!")
                break
            loops += 1
        client.record = record
        # print("Sent data to next node")


def initialize_client_topology(client):
    '''
        Connecting socket between current client and the neighboring client
        Once connection is made a new thread will be created to listen to that connection
    '''
    client_server = UDPServer(client.client_port)

    try:
        client_server.socket.bind((client.client_ip, client.client_port))
    except Exception as error:
            print(error)
            print(f"server: bind() failed for client: ip: {client.client_ip} port: {client.client_port} ")
            return
        
    # Add loop here so that we can disconnect and reconnect to server
    while True:

        print(f"client-server: Port server is listening to is: {client.client_port}\n")
        
        message, addr = client_server.socket.recvfrom(BUFFER_SIZE)

        client_topology(client_server, client, message, addr)


def client_topology(conn, client, data, addr):
    '''
        Will keep the connection with neighboring client until there is a disconnect on their end
    '''
    if data:
        data_loaded = data.decode('utf-8')
        try:
            data_loaded = json.loads(data_loaded)
            # print(f"client-topology: received message ``{data_loaded}''\n")
            if data_loaded['type'] == 'record':
                check_record(client, record=data_loaded['data'])
        except Exception as error:
            print(f"The following error occurred: {error}")


def main(args):
    if not (len(args) == 6):
        sys.exit(f"Usage: {args[0]} <Server IP address> <Server Port> <Client IP address> <Client Port> <Client Query Port>\n")
    
    serv_IP = args[1]  # First arg: server IP address (dotted decimal)
    echo_serv_port = int(args[2])  # Second arg: Use given port
    client_IP = args[3]
    client_port = int(args[4])
    query_ip = client_IP
    query_port = int(args[5])

    client = Client(serv_IP, echo_serv_port, client_IP, client_port, query_ip, query_port)

    # Start the client server
    print('Starting client topology socket\n')
    start_new_thread(initialize_client_topology, (client, ))

    # Start the client query server
    print("Starting client query socket\n")
    start_new_thread(client_query_socket, (client, ))
        

    # Start socket for connection to main server
    print(f"client: Arguments passed: server IP {client.serv_ip}, port {client.serv_port}\n")
    client_to_server = SocketInfo(client.serv_ip, client.serv_port)

    

    while True:            
        echo_string = input("\nEnter command for the server: ")

        
        if echo_string and echo_string != 'listen':
            # print(f"\nClient: reads string ``{echo_string}''\n")
            echo_string = bytes(echo_string, 'utf-8')
            try:
                s.sendall(echo_string)
                listen(s, client)
            except Exception as error:
                print(error)
                die_with_error("client: sendall() error")
        elif echo_string != 'listen':
            die_with_error("client: error reading string to echo\n")
        else:
            print('Listening for server incoming data\n')
            while True:
                listen(s, client)


if __name__ == "__main__":
    main(sys.argv)