from Server import UDPServer
from csv import DictReader
import os
from _thread import *
import json
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
        self.local_hash_table = setup_local_hash_table()
        self.user_dht = None

    def set_data(self, data):
        print('setting client data:', data)
        self.id = data[0]['id']
        self.user_dht = data
        self.username = data[0]['username']
        self.n = data[0]['n']
        self.next_node_addr = (data[1]['ip'], int(data[1]['port']))
        self.next_node_query_addr = (data[1]['ip'], int(data[1]['query']))


def setup_local_hash_table():
    '''Simple function used to initialize the local hash table to the constant size'''
    return [ [] for _ in range(HASH_SIZE) ]


def die_with_error(error_message):
    '''Function to kill the program and ouput the error message'''
    sys.exit(error_message)


def setup_all_local_dht(client):
    '''This function will read in the records one by one and call to check the record'''
    with open(os.path.join(sys.path[0], "StatsCountry.csv"), "r") as data_file:
        csv_reader = DictReader(data_file)
        total_records = 0
        # Iterate over each row in the csv using reader object
        for record in csv_reader:
            check_record(client, record)
            total_records += 1
            if total_records % 20 == 0:
                print(f"{total_records} read so far...")
        print(f"{total_records} read in total")


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
            time.sleep(0.5)
            # print(f'\nawaiting record to send: {loops}')
            if loops > 4:
                # print("\nTimeout occurred!\n\n")
                print("The nodes lost connection with eachother while trying to transfer the records!")
                break
            loops += 1

        client.record = record
        # print("Sent data to next node")


def initialize_client_topology(client):
    '''
        Connecting socket between current client and the neighboring client
        Once connection is made a new thread will be created to listen to that connection
    '''
    client_server = UDPServer()

    try:
        client_server.socket.bind(client.client_addr)
    except Exception as error:
            print(error)
            print(f"server: bind() failed for client: {client.client_addr}")
            return
        
    print(f"client-server: Port server is listening to is: {client.client_addr[1]}\n")
    start_new_thread(connect_nodes, (client_server, client))
    
    # Add loop here so that we can disconnect and reconnect to server
    while True:
        
        message, addr = client_server.socket.recvfrom(BUFFER_SIZE)

        # print(f"Client-server received message from addr: {addr}")

        client_topology(client_server, client, message, addr)


def client_topology(client_server, client, data, addr):
    '''
        Will keep the connection with neighboring client until there is a disconnect on their end
    '''
    if data:
        data_loaded = data.decode('utf-8')
        # try:
        data_loaded = json.loads(data_loaded)
        # print(f"client-topology: received message ``{data_loaded}''\n")
        if data_loaded['type'] == 'record':
            check_record(client, record=data_loaded['data'])
        elif data_loaded['type'] == 'set-id':
            client.set_data(data_loaded['data'])
            
            client_server.socket.sendto(b'SUCCESS', addr)
        # except Exception as error:
        #     print(f"The following error occurred: {error}")


def run_query(s, addr, client, long_name):
    '''
        Take in the query command and either return response with record found
        or call the next node with the same query command
    '''
    pos = hash_pos({'Long Name': ' '.join(long_name)})
    print('n: ', client.n)
    id = pos % client.n
    if id == client.id:
        # This is the correct node for query
        records = client.local_hash_table[pos]
        for record in records:
            if record['Long Name'] == ' '.join(long_name):
                s.send_response(addr, res='SUCCESS', type='query-result', data=record)
                return
        
        s.send_response(addr, res='FAILURE', type='query-result')
    else:
        # This isn't the correct node for query
        client.query = ' '.join(long_name)
        connect_query_nodes(s, client, addr)
    

def connect_query_nodes(s, client, origin, ip=None, port=None):
    '''
        Connect to given query address or the query address of the next node
    '''
    # This check will avoid looping through the nodes infinitely
    if client.began_query and not ip:
        client.began_query = False
        print("Query looped through and didn't find record")
        s.send_response(origin, res='FAILURE', type='query-result')
        

    if client.query:
        query_info = 'query ' + client.query
        data = {
            'data': query_info,
            'origin': origin
        }
        data_loaded = json.dumps(data)
        query = bytes(data_loaded, 'utf-8')
        client.query = None
        
        try:
            if ip:
                print(f'sending query info {data_loaded} to address {ip}, {port}')
                client.began_query = True
                s.socket.sendto(query, (ip, port))
            else:
                print(f'sending query info {data_loaded} to address {client.next_node_query_addr}')
                s.socket.sendto(query, client.next_node_query_addr)
            # Sent query now listening for response from next node!
            # query_response = query_listen(s)
            # return query_response
        except:
            die_with_error("client-node: sendall() error within query connection")
        # else:
        #     time.sleep(1)
    else:
        print("missing query be sure to put 'query {Long Name}' in your query command")
        s.send_response(origin, res='FAILURE', type='query-result')


def client_query_socket(client):
    '''
        Set up the socket for query port which will accept all connections and then start
        a new listening thread
    '''
    client_query = UDPServer()

    try:
        client_query.socket.bind(client.query_addr)
    except Exception as error:
            print(error)
            print("query-server: bind() failed")
            return
   
        
    # Add loop here so that we can disconnect and reconnect to server
    print(f"query-server: Port server is listening to is: {client.query_addr[1]}\n")
    
    while True:
        
        message = client_query.socket.recv(BUFFER_SIZE)

        # print(f"Query port received message: {message} from addr: {addr}")

        client_query_conn(client_query, client, message)


def client_query_conn(client_query, client, data):
    '''
        Socket connection listener that will listen for query commands
    '''

    if data:
        data_loaded = data.decode('utf-8')

        if data_loaded:
            try:
                data_loaded = json.loads(data_loaded)
            except:
                print("error with json.load")
                return

        if type(data_loaded['data']) != str:
            print(data_loaded['data'])
            return
        data_list = data_loaded['data'].split()
        # print(f"query-conn: received message ``{data_list}''\n")
        if data_list[0] == 'query':
            addr = tuple(data_loaded['origin'])
            run_query(client_query, addr, client, data_list[1:])
        else:
            print(f"Incorrect data received at query port {data_loaded}")


def connect_all_nodes(s, client):
    i = 1

    while i < len(client.user_dht):
        if i + 1 < len(client.user_dht):
            data = (client.user_dht[i], client.user_dht[i+1])
        else:
            data = (client.user_dht[i], client.user_dht[0])
        # print("Sending data: ", data)
        # print("\n\n\n")
        
        try:
            s.send_response(addr=(client.user_dht[i]['ip'], int(client.user_dht[i]['port'])), res='SUCCESS', type='set-id', data=data)
            s.socket.recv(BUFFER_SIZE)
            print(f"Successfully sent set-id command to {client.user_dht[i]['username']}")
        except Exception as error:
            print(error)
            print('An exception occurred sending node connection data')
        
        i += 1


def connect_nodes(s, client):
    '''
        This function will connect the current node with the node to the right
        in the DHT
    '''
    print("Successfully began loop to send records to next node!\n Awaiting records to forward\n\n")
    while True:
        if client.record:
            try:
                # print(f"sending to next node addr {client.next_node_addr}")
                s.send_response(addr=client.next_node_addr, res='SUCCESS', type='record', data=client.record)
            except:
                print("client-node: sendall() error within records connect nodes")
            client.record = None


def listen(s, client):
    '''
        Listen function that will listen to all responses from the server connected to client
    '''
    while True:
        data = s.socket.recv(BUFFER_SIZE)
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
                client.set_data(data_loaded['data'])
                connect_all_nodes(s, client)
                setup_all_local_dht(client)
                success_string = bytes(f'dht-complete {client.username}', 'utf-8')
                try:
                    s.socket.sendto(success_string, client.server_addr)
                except:
                    die_with_error("client: sendall() error sending success string")
            elif data_loaded['type'] == 'query-response':
                query_long_name = input("Enter query followed by the Long Name to query: ")
                client.query = ' '.join(query_long_name.split()[1:])
                # print(f"Received query of {client.query}")
                first_ip = data_loaded['data']['ip']
                first_port = int(data_loaded['data']['query'])
                connect_query_nodes(s, client, origin=client.query_addr, ip=first_ip, port=first_port)
                # print(response)
        else:
            print(data_loaded)


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
    print(f"client: Arguments passed: server IP {client.server_addr}\n")
    # client_to_server = SocketInfo(client.serv_ip, client.serv_port)
    client_to_server = UDPServer()
    

    print("Starting thread to listen to server\n")
    start_new_thread(listen, (client_to_server, client))
    
    while True:
        time.sleep(0.5)
        echo_string = input("\nEnter command for the server: ")

        if echo_string:
            # print(f"\nClient: reads string ``{echo_string}''\n")
            echo_string = bytes(echo_string, 'utf-8')
            try:
                client_to_server.socket.sendto(echo_string, client.server_addr)
                
            except Exception as error:
                print(error)
                die_with_error("client: sendall() error")
        else:
            print("client: error reading string to forward to server\n")
                


if __name__ == "__main__":
    main(sys.argv)