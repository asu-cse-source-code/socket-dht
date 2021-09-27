from server import UDPServer
from csv import DictReader
import os
from _thread import *
import json
import sys


class Client:
    '''The Client class that has a single instance for each client running'''
    def __init__(self, serv_ip, serv_port, client_ip, client_port, query_ip, query_port, right_ip, right_port, hash_size, buff_size):
        self.BUFFER_SIZE = buff_size
        self.HASH_SIZE = hash_size
        self.server_addr = (serv_ip, serv_port)
        self.accept_port_address = (client_ip, client_port)
        self.query_addr = (query_ip, query_port)
        self.send_port_addr = (right_ip, right_port)
        self.next_node_addr = None
        self.next_node_query_addr = None
        self.record = None
        self.query = None
        self.began_query = False
        self.id = None
        self.username = None
        self.n = None
        self.local_hash_table = [ [] for _ in range(hash_size) ]
        self.user_dht = None
        self.terminate = False
        # UPDServer sockets
        self.client_to_server = UDPServer()
        self.accept_port = UDPServer()
        self.query_port = UDPServer()
        self.send_port = UDPServer()


    def set_data(self, data):
        print('setting client data:', data)
        self.id = data[0]['id']
        self.user_dht = data
        self.username = data[0]['username']
        self.n = data[0]['n']
        self.next_node_addr = (data[1]['ip'], int(data[1]['port']))
        self.next_node_query_addr = (data[1]['ip'], int(data[1]['query']))

    def hash_pos(self, record):
        '''Calculate the pos variable with this hash function'''
        ascii_sum = 0
        for letter in record['Long Name']:
            ascii_sum += ord(letter)
        
        return ascii_sum % self.HASH_SIZE

    def check_record(self, record):
        '''
            Check given record and see if it is stored on the local hash table
            If it is not set the self.record value to the record which will
            trigger the query socket and send the query command to next node
        '''
        pos = self.hash_pos(record)
        id = pos % self.n
        if id == self.id:
            # This is the desired location for record!
            self.local_hash_table[pos].append(record)
        else:
            try:
                # print(f"sending to next node addr {client.next_node_addr}")
                self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='record', data=record)
            except:
                print("client-node: sendall() error within records connect nodes")

    def initialize_client_topology(self):
        '''
            Connecting socket between current client and the neighboring client
            Once connection is made a new thread will be created to listen to that connection
        '''

        try:
            self.accept_port.socket.bind(self.accept_port_address)
        except Exception as error:
                print(error)
                print(f"server: bind() failed for client: {self.accept_port_address}")
                return
            
        print(f"client-server: Port server is listening to is: {self.accept_port_address[1]}\n")
        
        # Add loop here so that we can disconnect and reconnect to server
        while True:
            
            message, addr = self.accept_port.socket.recvfrom(self.BUFFER_SIZE)

            # print(f"Client-server received message from addr: {addr}")

            self.client_topology(message, addr)

    def client_topology(self, data, addr):
        '''
            Will keep the connection with neighboring client until there is a disconnect on their end
        '''
        if data:
            data_loaded = data.decode('utf-8')
            data_loaded = json.loads(data_loaded)
            # print(f"client-topology: received message ``{data_loaded}''\n")
            if data_loaded['type'] == 'record':
                self.check_record(record=data_loaded['data'])
            elif data_loaded['type'] == 'set-id':
                self.set_data(data_loaded['data'])
                self.accept_port.socket.sendto(b'SUCCESS', addr)

    def client_query_socket(self):
        '''
            Set up the socket for query port which will accept all connections and then start
            a new listening thread
        '''

        try:
            self.query_port.socket.bind(self.query_addr)
        except Exception as error:
                print(error)
                print("query-server: bind() failed")
                return
    
            
        # Add loop here so that we can disconnect and reconnect to server
        print(f"query-server: Port server is listening to is: {self.query_addr[1]}\n")
        
        while True:
            
            message = self.query_port.socket.recv(self.BUFFER_SIZE)

            # print(f"Query port received message: {message} from addr: {addr}")

            self.client_query_conn(message)


    def client_query_conn(self, data):
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
                self.run_query(addr, data_list[1:])
            else:
                print(f"Incorrect data received at query port {data_loaded}")


    def run_query(self, addr, long_name):
        '''
            Take in the query command and either return response with record found
            or call the next node with the same query command
        '''
        pos = self.hash_pos({'Long Name': ' '.join(long_name)})

        id = pos % self.n
        if id == self.id:
            # This is the correct node for query
            records = self.local_hash_table[pos]
            for record in records:
                if record['Long Name'] == ' '.join(long_name):
                    self.query_port.send_response(addr, res='SUCCESS', type='query-result', data=record)
                    return
            
            self.query_port.send_response(addr, res='FAILURE', type='query-result')
        else:
            # This isn't the correct node for query
            self.query = ' '.join(long_name)
            self.connect_query_nodes(addr)


    def connect_query_nodes(self, origin, ip=None, port=None):
        '''
            Connect to given query address or the query address of the next node
        '''
        # This check will avoid looping through the nodes infinitely
        if self.began_query and not ip:
            self.began_query = False
            print("Query looped through and didn't find record")
            self.query_port.send_response(origin, res='FAILURE', type='query-result')
            

        if self.query:
            query_info = 'query ' + self.query
            data = {
                'data': query_info,
                'origin': origin
            }
            data_loaded = json.dumps(data)
            query = bytes(data_loaded, 'utf-8')
            self.query = None
            
            try:
                if ip:
                    print(f'sending query info {data_loaded} to address {ip}, {port}')
                    self.began_query = True
                    self.query_port.socket.sendto(query, (ip, port))
                else:
                    print(f'sending query info {data_loaded} to address {self.next_node_query_addr}')
                    self.query_port.socket.sendto(query, self.next_node_query_addr)
                # Sent query now listening for response from next node!
            except:
                print("client-node: sendall() error within query connection")
                return
            # else:
            #     time.sleep(1)
        else:
            print("missing query be sure to put 'query {Long Name}' in your query command")
            self.query_port.send_response(origin, res='FAILURE', type='query-result')

    def connect_all_nodes(self):
        i = 1

        while i < len(self.user_dht):
            if i + 1 < len(self.user_dht):
                data = (self.user_dht[i], self.user_dht[i+1])
            else:
                data = (self.user_dht[i], self.user_dht[0])
            # print("Sending data: ", data)
            try:
                self.send_port.send_response(addr=(self.user_dht[i]['ip'], int(self.user_dht[i]['port'])), res='SUCCESS', type='set-id', data=data)
                self.send_port.socket.recv(self.BUFFER_SIZE)
                print(f"Successfully sent set-id command to {self.user_dht[i]['username']}")
            except Exception as error:
                print(error)
                print('An exception occurred sending node connection data')
            
            i += 1
