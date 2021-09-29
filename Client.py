from Server import UDPServer
from csv import DictReader
import os
from _thread import *
import json
import sys


class Client:
    '''The Client class that has a single instance for each client running'''
    def __init__(self, username, serv_ip, serv_port, client_ip, client_port, query_ip, 
                query_port, right_ip, right_port, hash_size, buff_size, file_path):
        self.BUFFER_SIZE = buff_size
        self.HASH_SIZE = hash_size
        self.FILE_PATH = file_path
        self.username = username
        self.server_addr = (serv_ip, serv_port)
        self.accept_port_address = (client_ip, client_port)
        self.query_addr = (query_ip, query_port)
        self.send_port_addr = (right_ip, right_port)
        self.next_node_addr = None
        self.next_node_query_addr = None
        self.prev_node_addr = None
        self.record = None
        self.query = None
        self.began_query = False
        self.id = None
        self.n = None
        self.local_hash_table = [ [] for _ in range(hash_size) ]
        self.user_dht = None
        self.new_leader = None
        self.leaving_user = False
        self.joining_user = False
        self.started_check = False
        # UPDServer sockets
        '''
            I should create separate sub classes within this Client class
        '''
        self.client_to_server = UDPServer()
        self.accept_port = UDPServer()
        self.query_port = UDPServer()
        self.send_port = UDPServer()


    def start_threads(self):
        # Start the client server
        print('Starting client topology socket\n')
        start_new_thread(self.initialize_acceptance_port, ())

        # Start the client query server
        print("Starting client query socket\n")
        start_new_thread(self.client_query_socket, ())
            

    def set_data(self, data, index=0):
        self.user_dht = data
        self.prev_node_addr = (data[index]['ip'], int(data[index]['port']))
        self.id = data[index+1]['id']
        # self.username = data[index+1]['username']
        self.n = data[index+1]['n']
        self.next_node_addr = (data[index+2]['ip'], int(data[index+2]['port']))
        self.next_node_query_addr = (data[index+2]['ip'], int(data[index+2]['query']))

    def num_of_records(self):
        count = 0
        for list_of_records in self.local_hash_table:
            for _ in list_of_records:
                count += 1

        return f"\tRecords held in hash: {count}"

    def output_node_info(self):
        print('\n\nNode info:\n\n')
        print(f"\tn = {self.n}")
        print(f"\tid = {self.id}")
        print(f"\tusername = {self.username}")
        print(f"\taccpt addr = {self.accept_port_address}")
        print(f"\tquery addr = {self.query_addr}")
        print(f"\tnext node = {self.next_node_addr}")
        print(f"\tprev node = {self.prev_node_addr}")
        print(f"\tnext query = {self.next_node_query_addr}")
        print(self.num_of_records())

    def hash_pos(self, record):
        '''Calculate the pos variable with this hash function'''
        ascii_sum = 0
        for letter in record['Long Name']:
            ascii_sum += ord(letter)
        
        return ascii_sum % self.HASH_SIZE
    
    def end_script(message):
        if message:
            print(message)
        sys.exit()

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

    def setup_all_local_dht(self, print_input=True):
        '''This function will read in the records one by one and call to check the record'''
        with open(os.path.join(sys.path[0], self.FILE_PATH), "r") as data_file:
            csv_reader = DictReader(data_file)
            total_records = 0
            # Iterate over each row in the csv using reader object
            print("\nSending records through DHT to store.\n")
            for record in csv_reader:
                self.check_record(record)
                total_records += 1
                if total_records % 50 == 0:
                    print(f"\t{total_records} records stored so far...")
            print(f"\n\t{total_records} records stored in total")
            if print_input:
                print("\nEnter command for the server: ")
    
    def teardown_dht(self, leaving):
        # Only teardown the local DHT, don't remove ID's or neighbors
        self.local_hash_table = [ [] for _ in range(self.HASH_SIZE) ]
        if not leaving:
            self.id = None
            self.n = None
            self.user_dht = None
            self.next_node_addr = None
            self.next_node_query_addr = None
            self.prev_node_addr = None

    def initialize_acceptance_port(self):
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

            self.client_acceptance(message, addr)

    def client_acceptance(self, data, addr):
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
            elif data_loaded['type'] == 'leaving-teardown':
                # Call teardown but with the leaving var set to True
                self.teardown_dht(True)
                if self.leaving_user:
                    # Now call to reset every node id
                    print('Teardown complete now calling reset-id\n')
                    self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='reset-id', data=0)
                elif self.joining_user:
                    print("Teardown complete now rebuilding the DHT\n")
                    # We know that the next node is the leader so call for rebuild of DHT
                    self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='rebuild-dht', data=self.accept_port_address)
                else:
                    # Continue with teardown
                    self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='leaving-teardown')
            elif data_loaded['type'] == 'teardown':
                if self.id == 0:
                    self.teardown_dht(False)
                    # Send successful command to server
                    self.client_to_server.socket.sendto(bytes(f'teardown-complete {self.username}', 'utf-8'), self.server_addr) 
                    self.listen()
                else:
                    next_node_addr = self.next_node_addr
                    self.teardown_dht(False)
                    # print(next_node_addr)
                    self.send_port.send_response(addr=next_node_addr, res='SUCCESS', type='teardown')
            elif data_loaded['type'] == 'reset-id':
                new_id = int(data_loaded['data'])
                if not self.leaving_user:
                    self.id = new_id
                    self.n = self.n - 1
                    new_id += 1
                    self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='reset-id', data=new_id)
                else:
                    # We know that the nodes have successfully been renumbered
                    print("Node ID's successfully changed")
                    # Call to convert neighbors or the nodes
                    self.convert_neighbors()
            elif data_loaded['type'] == 'reset-n':
                if self.id == 0:
                    # This is leader so set previous node and the new n
                    self.n = self.n + 1
                    data_loaded['data']['n'] = self.n
                    self.prev_node_addr = data_loaded['data']['addr']
                    self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='reset-n', data=data_loaded['data'])
                elif self.username != data_loaded['data']['username']:
                    self.n = self.n + 1
                    if self.n - 2 == self.id:
                        data_loaded['data']['prev'] = self.accept_port_address
                        self.next_node_addr = tuple(data_loaded['data']['addr'])
                        self.next_node_query_addr = tuple(data_loaded['data']['query'])
                    self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='reset-n', data=data_loaded['data'])
                else:
                    # We know that the nodes have successfully been renumbered
                    print("Node size successfully changed\n")
                    self.prev_node_addr = data_loaded['data']['prev']
                    self.n = data_loaded['data']['n']
                    self.id = self.n - 1

                    print("Teardown the existing DHT\n")
                    # Teardown the current DHT
                    self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='leaving-teardown')
            elif data_loaded['type'] == 'reset-left':
                # print(data_loaded['data'])
                if self.next_node_addr == tuple(data_loaded['data']['current']):
                    # print("This is the prev node")
                    self.next_node_addr = tuple(data_loaded['data']['new'])
                    self.next_node_query_addr = tuple(data_loaded['data']['query'])
                    self.send_port.send_response(addr=tuple(data_loaded['data']['current']), res='SUCCESS', type='reset-complete')
                else:
                    self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='reset-left', data=data_loaded['data'])
            elif data_loaded['type'] == 'reset-right':
                self.prev_node_addr = tuple(data_loaded['data'])
                self.send_port.socket.sendto(bytes(self.username, 'utf-8'), addr)
            elif data_loaded['type'] == 'reset-complete':
                print('Received reset complete\nNow rebuilding DHT')
                res_data = self.accept_port_address
                self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='rebuild-dht', data=res_data)
            elif data_loaded['type'] == 'rebuild-dht':
                print("Received rebuild DHT command\nSetting up node ring")
                # self.new_leader = self.username # Set new leader when we initialize the rebuild of DHT
                self.setup_all_local_dht()
                self.send_port.send_response(addr=tuple(data_loaded['data']), res='SUCCESS', type='dht-rebuilt')
            elif data_loaded['type'] == 'dht-rebuilt':
                success_string = bytes(f'dht-rebuilt {self.username} {self.new_leader}', 'utf-8')
                if self.joining_user:
                    self.joining_user = False
                    success_string = bytes(f'dht-rebuilt {self.username}', 'utf-8')
                self.leaving_user = False
                
                try:
                    self.client_to_server.socket.sendto(success_string, self.server_addr)
                    self.listen()
                except:
                    print("client: sendall() error sending success string")
                    return
            elif data_loaded['type'] == 'check-nodes':
                self.output_node_info()
                if not self.started_check:
                    self.started_check = False
                else:
                    self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='check-nodes')


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
                record = data_loaded['data']
                if record == None:
                    print(f"\n\nQuery for Long Name of {self.query}: 404 record not found\n")
                else:
                    print(f"\n\nQuery for Long Name of {self.query}:\n")
                    print(json.dumps(data_loaded['data'], sort_keys=False, indent=4))
                self.query = None
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
                    self.began_query = False
                    self.query_port.send_response(addr, res='SUCCESS', type='query-result', data=record)
                    return
            
            self.began_query = False
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
            return

        if self.query:
            query_info = 'query ' + self.query
            data = {
                'data': query_info,
                'origin': origin
            }
            data_loaded = json.dumps(data)
            query = bytes(data_loaded, 'utf-8')
            
            try:
                if ip:
                    # print(f'sending query info {data_loaded} to address {ip}, {port}')
                    self.began_query = True
                    self.query_port.socket.sendto(query, (ip, port))
                else:
                    # print(f'sending query info {data_loaded} to address {self.next_node_query_addr}')
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
            j = i - 1
            if i + 1 < len(self.user_dht):
                data = (self.user_dht[j], self.user_dht[i], self.user_dht[i+1])
            else:
                data = (self.user_dht[j], self.user_dht[i], self.user_dht[0])
            # print("Sending data: ", data)
            try:
                print(f'sending message to addr: ({self.user_dht[i]["ip"]}, {self.user_dht[i]["port"]})')
                self.send_port.send_response(addr=(self.user_dht[i]['ip'], int(self.user_dht[i]['port'])), res='SUCCESS', type='set-id', data=data)
                self.send_port.socket.recv(self.BUFFER_SIZE)
                print(f"Successfully sent set-id command to {self.user_dht[i]['username']}")
            except Exception as error:
                print(error)
                print('An exception occurred sending node connection data')
            
            i += 1

    def convert_neighbors(self):
        reset_right_data = self.prev_node_addr

        # Send the reset-right command and await a response
        self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='reset-right', data=reset_right_data)
        res = self.send_port.socket.recv(self.BUFFER_SIZE)

        # Check for success message from res
        data_loaded = res.decode('utf-8')
        # print(f'REset right sent back {data_loaded}')
        self.new_leader = data_loaded

        reset_left_data = {
            'current': self.accept_port_address,
            'new': self.next_node_addr,
            'query': self.next_node_query_addr
        }
        
        self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='reset-left', data=reset_left_data)        
        
    def check_nodes(self):
        self.started_check = True
        self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='check-nodes')

    def listen(self):
        '''
            Listen function that will listen to all responses from the server connected to self
        '''
        
        data = self.client_to_server.socket.recv(self.BUFFER_SIZE)
        data_loaded = data.decode('utf-8')

        if data_loaded:
            try:
                data_loaded = json.loads(data_loaded)
            except:
                print("error with json.load")
                return
        
        print("\n\n")
        if data_loaded and data_loaded['res'] == 'SUCCESS':
            if data_loaded['data']:
                print(json.dumps(data_loaded, sort_keys=False, indent=4))
            
            if data_loaded['type'] == 'DHT':
                self.set_data(data_loaded['data'], index=-1)
                self.connect_all_nodes()
                # Call setup all dht but set the input printer var to false
                self.setup_all_local_dht(False)
                success_string = bytes(f'dht-complete {self.username}', 'utf-8')
                try:
                    self.client_to_server.socket.sendto(success_string, self.server_addr)
                    self.listen()
                except:
                    print("client: sendall() error sending success string")
                    return
            elif data_loaded['type'] == 'query-response':
                query_long_name = input("Enter query followed by the Long Name to query: ")
                self.query = ' '.join(query_long_name.split()[1:])
                # print(f"Received query of {self.query}")
                first_ip = data_loaded['data']['ip']
                first_port = int(data_loaded['data']['query'])
                self.connect_query_nodes(origin=self.query_addr, ip=first_ip, port=first_port)
                # print(response)
            elif data_loaded['type'] == 'join-response':
                self.joining_user = True
                self.username = data_loaded['data']['username']
                self.next_node_addr = tuple(data_loaded['data']['leader'][0])
                self.next_node_query_addr = tuple(data_loaded['data']['leader'][1])
                
                new_data = {
                    'username': self.username,
                    'n': 0,
                    'addr': self.accept_port_address,
                    'query': self.query_addr
                }

                # print('Teardown complete now calling reset-n\n')
                self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='reset-n', data=new_data)
            elif data_loaded['type'] == 'deregister':
                self.end_script(f"{data_loaded['data']}\nTerminating client application.")
            elif data_loaded['type'] == 'leave-response':
                self.leaving_user = True
                self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='leaving-teardown')
            elif data_loaded['type'] == 'teardown-response':
                # Need to be on the leader node for this to work
                if self.id == 0:
                    self.send_port.send_response(addr=self.next_node_addr, res='SUCCESS', type='teardown')
                else:
                    print("\n\nCan't run this command since this is not the leader node\n")
        else:
            print(json.dumps(data_loaded, sort_keys=False, indent=4))
