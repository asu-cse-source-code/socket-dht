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
    def __init__(self, serv_ip, serv_port, client_ip, client_port, query_ip, query_port):
        self.serv_ip = serv_ip
        self.serv_port = serv_port
        self.client_ip = client_ip
        self.client_port = client_port
        self.query_ip = query_ip
        self.query_port = query_port
        self.next_node_ip = None
        self.next_node_port = None
        self.next_node_query_ip = None
        self.next_node_query_port = None
        self.record = None
        self.query = None
        # self.client_conn = None
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


def run_query(client, long_name):
    pos = hash_pos({'Long Name': ' '.join(long_name)})
    id = pos % client.n
    if id == client.id:
        print("This is the correct node for query")
        records = client.local_hash_table[pos]
        for record in records:
            if record['Long Name'] == ' '.join(long_name):
                return 'SUCCESS', record
        
        return 'FAILURE', None
        
    else:
        print("This isn't the correct node for query")
        client.query = ' '.joing(long_name)
        connect_nodes(client, 'query')



def check_record(client, record):
    pos = hash_pos(record)
    id = pos % client.n
    if id == client.id:
        print("This is the desired location for record!")
        client.local_hash_table[pos].append(record)
    else:
        print("This is not the desired location for the record")
        print(f'The desired location is on id: {id}')
        loops = 1
        while client.record:
            time.sleep(1)
            print(f'\nawaiting record to send: {loops}')
            if loops > 4:
                print("\nTimeout occurred!\n\n")
                break
            loops += 1
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


def connect_nodes(client, purpose):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if purpose == 'records':
            s.connect((client.next_node_ip, int(client.next_node_port)))

            print("Successfully connected with next node!\n Awaiting records to forward\n\n")
            while True:
                if client.record:
                    response_data = json.dumps({
                        'res': 'SUCCESS',
                        'type': 'record',
                        'data': client.record,
                    })
                    record = bytes(response_data, 'utf-8')
                    client.record = None
                    try:
                        s.sendall(record)
                    except:
                        die_with_error("client-node: sendall() error")
                # else:
                #     time.sleep(1)
        elif purpose == 'query':
            for item in client.user_dht:
                if item[1] == client.next_node_ip:
                    client.next_node_query_ip = item[1]
                    client.next_node_query_port = item[4]
            s.connect((client.next_node_query_ip, int(client.next_node_query_port)))

            print("Successfully connected with next node!\n Awaiting query to forward\n\n")
            
            if client.query:
                query_info = 'query ' + client.query
                query = bytes(query_info, 'utf-8')
                client.query = None
                try:
                    s.sendall(query)
                    print('Sent query now listening for response from next node!\n')
                    query_response = query_listen(s)
                    return query_response
                except:
                    die_with_error("client-node: sendall() error")
                    return None
                # else:
                #     time.sleep(1)
            else:
                print("missing query")
                return None


def query_listen(s):
    data = s.recv(BUFFER_SIZE)
    data_loaded = data.decode('utf-8')

    if data_loaded:
        try:
            data_loaded = json.loads(data_loaded)
        except:
            print("error with json.load")
            return None
        print(f'Query response: {data_loaded}\n')
        return data_loaded
    else:
        return None


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
            client.user_dht = data_loaded['data']
        elif data_loaded['type'] == 'topology':
            # Set some client values
            client.username = data_loaded['data']['username']
            client.n = data_loaded['data']['n']
            client.id = data_loaded['data']['id']
            client.next_node_ip = data['ip']
            client.next_node_port = int(data['port'])

            start_new_thread(connect_nodes, (client, 'records' ))
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
                try:
                    data_loaded = json.loads(data_loaded)
                    print(f"client-topology: received message ``{data_loaded}''\n")
                    if data_loaded['type'] == 'record':
                        check_record(client, record=data_loaded['data'])
                except:
                    print("error with json.load")
            else:
                break


def client_query_socket(client):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((client.query_ip, client.query_port))
        except:
            die_with_error("query-server: bind() failed")
        
        # Add loop here so that we can disconnect and reconnect to server
        while True:
        
            s.listen()

            print(f"query-server: Port server is listening to is: {client.query_port}\n")
            
            conn, addr = s.accept()

            print('Connected by', addr)

            start_new_thread(client_query_conn, (client, conn, ))


def client_query_conn(client, conn):
    with conn:
        while True:
            data = conn.recv(1024)

            if data:
                data_list = data.decode('utf-8').split()
                print(f"query-conn: received message ``{data_list}''\n")
                if data_list[0] == 'query':
                    response, record = run_query(client, data_list[1:])

                else:
                    response_data = json.dumps({
                            'res': 'FAILURE',
                            'data': None
                        })
                    conn.sendall(bytes(response_data, 'utf-8'))
            else:
                break


def main(args):
    if len(args) < 3:
        die_with_error(f"Usage: {args[0]} <Server IP address> <Server Port> <Client IP address> <Client Port>\n")
    
    serv_IP = args[1]  # First arg: server IP address (dotted decimal)
    echo_serv_port = int(args[2])  # Second arg: Use given port
    client_port = client_IP = None
    query_ip = query_port = None
    if (len(args) > 3):
        client_IP = args[3]
        client_port = int(args[4])
        query_ip = args[5]
        query_port = int(args[6])

    client = Client(serv_IP, echo_serv_port, client_IP, client_port, query_ip, query_port)

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
                    # print(client.local_hash_table)
            
            listen(s, client)


if __name__ == "__main__":
    main(sys.argv)