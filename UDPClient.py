from csv import DictReader
import os
from _thread import *
import json
import socket
import sys
import time

HASH_SIZE = 353 # Size to initialize the local hash table to
BUFFER_SIZE = 3072 # Max bytes to take in


class Client:
    def __init__(self, serv_ip, serv_port, client_ip, client_port):
        self.serv_ip = serv_ip
        self.serv_port = serv_port
        self.client_ip = client_ip
        self.client_port = client_port
        self.record = None
        self.client_conn = None
        self.id = None
        self.username = None
        self.n = None
        self.local_hash_table = setup_local_hash_table()
        self.user_dht = None


def die_with_error(error_message):
    sys.exit(error_message)


def hash_pos(record):
    ascii_sum = 0
    for letter in record['Long Name']:
        ascii_sum += ord(letter)
    
    return ascii_sum % 353


def check_record(client, record):
    pos = hash_pos(record)
    id = pos % client.n
    if id == client.id:
        print("This is the desired location for record!")
        client.local_hash_table[pos].append(record)
    else:
        print("This is not the desired location for the record")
        print(f'The desired location is on id: {id}')
        client.record = record
        print("Sent data to next node")


def setup_all_local_dht(client):
    with open(os.path.join(sys.path[0], "StatsCountry.csv"), "r") as data_file:
        csv_reader = DictReader(data_file)
        # Iterate over each row in the csv using reader object
        for record in csv_reader:
            check_record(client, record)
            
            

def setup_local_hash_table():
    return [ [] for _ in range(HASH_SIZE) ]


def connect_nodes(client, data):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((data['ip'], int(data['port'])))

        print("Successfully connected with next node!\n Awaiting records to forward\n\n")

        while True:
            if client.record:
                response_data = json.dumps({
                    'res': 'SUCCESS',
                    'type': 'record',
                    'data': record,
                })
                record = bytes(response_data, 'utf-8')
                client.record = None
                try:
                    s.sendall(record)
                except:
                    die_with_error("client-node: sendall() error")
            else:
                time.sleep(1)


def listen(s, client):
    data = s.recv(BUFFER_SIZE)
    data_loaded = data.decode('utf-8')

    if data_loaded:
        try:
            data_loaded = json.loads(data_loaded)
        except:
            print("error with json.load")
            return
    print(data_loaded)
    
    if data_loaded and data_loaded['res'] == 'SUCCESS':
        print("client: received SUCCESS response from server")
        if data_loaded['data']:
            print(f"\nclient: received data {data_loaded['data']} from server on IP address {client.serv_ip}\n")
        
        if data_loaded['type'] == 'DHT':
            user_dht = data_loaded['data']
        elif data_loaded['type'] == 'topology':
            # Set some client values
            client.username = data_loaded['data']['username']
            client.n = data_loaded['data']['n']
            client.id = data_loaded['data']['id']
            start_new_thread(connect_nodes, (client, data_loaded['data']))
            print("Began node connection thread")
            if (client.id == 0):
                setup_all_local_dht(client)
                success_string = bytes(f'dht-complete {client.username}', 'utf-8')
                try:
                    s.sendall(success_string)
                except:
                    die_with_error("client: sendall() error sending success string")
    # else:
        # die_with_error("client: recvfrom() failed")


def initialize_client_topology(client):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((client.client_ip, client.client_port))
        except Exception as error:
            print(error)
            print(f"server: bind() failed for client: ip: {client.client_ip} port: {client.client_port} ")
            return
    
        # Add loop here so that we can disconnect and reconnect to clients
        while True:
        
            sock.listen()

            print(f"client-server: Port server is listening to is: {client.client_port}\n")
            
            conn, addr = sock.accept()

            print('Connected by', addr)

            start_new_thread(client_topology, (conn, client))


def client_topology(conn, client):
    with conn:
        while True:
            data = conn.recv(BUFFER_SIZE)

            if data:
                data_loaded = data.decode('utf-8')
                data_loaded = json.loads(data_loaded)
                print(f"client-topology: received message ``{data_loaded}''\n")
                if data_loaded['type'] == 'record':
                    check_record(client, record=data_loaded['data'])
            else:
                break



def main(args):
    if len(args) < 3:
        die_with_error(f"Usage: {args[0]} <Server IP address> <Server Port> <Client IP address> <Client Port>\n")
    
    serv_IP = args[1]  # First arg: server IP address (dotted decimal)
    echo_serv_port = int(args[2])  # Second arg: Use given port
    client_IP = None
    client_port = None
    if (len(args) > 3):
        client_IP = args[3]
        client_port = int(args[4])

    client = Client(serv_IP, echo_serv_port, client_IP, client_port)


    user_dht = []

    if client_port:
        print('Starting client topology socket\n')
        start_new_thread(initialize_client_topology, (client, ))
        


    print(f"client: Arguments passed: server IP {client.serv_ip}, port {client.serv_port}\n")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((client.serv_ip, client.serv_port))

        while True:            
            echo_string = input("\nEnter command for the server: ")

            if echo_string and echo_string != 'listen':
                print(f"\nClient: reads string ``{echo_string}''\n")
                echo_string = bytes(echo_string, 'utf-8')
                try:
                    s.sendall(echo_string)
                except:
                    die_with_error("client: sendall() error")
            elif echo_string != 'listen':
                die_with_error("client: error reading string to echo\n")
            else:
                print('Listening for server incoming data\n')
                while True:
                    listen(s, client)
                    print(client.local_hash_table)
            
            listen(s, client)


if __name__ == "__main__":
    main(sys.argv)