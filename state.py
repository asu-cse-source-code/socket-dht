import json
import random


class StateInfo:
    def __init__(self, port):
        self.state_table = {} # Initialize empty dictionary for the state table
        self.server_port = port
        self.ports = [port]
        self.dht_flag = False
        self.creating_dht = False
        self.stabilizing_dht = False
        self.tearing_down_dht = False
        self.dht_leader = None
        self.leaving_user = None

    class User:
        '''The User class will have as many instances as users registered'''
        def __init__(self, username, ip_address, ports):
            self.username = username
            self.ipv4 = ip_address
            # Convert port to integers
            self.client_port = int(ports[0])
            self.client_query_port = int(ports[1])
            self.state = 'Free'
    
    def reset_dht(self):
        # Set every user to state of Free
        for username in self.state_table.keys():
            self.state_table[username].state = 'Free'
        
        self.dht_flag = False
        self.tearing_down_dht = False
        self.dht_leader = None
        self.ports = [self.server_port]

    def valid_user(self, user):
        '''Helper function to check if the user given is valid for registry'''
        # Check if username already exists and also if the username is all alphabetical
        if user in self.state_table.keys():
            return "User already registered"

        if not user.isalpha():
            return "Username must be an alphabetic string"
        
        return None

    def register(self, data_list):
        '''
            This function will take in the command from client and check 
            if the given information is valid to register a new user

            ex: register ⟨username⟩ ⟨IP⟩ ⟨acceptance port⟩ ⟨query port⟩
        '''
        if len(data_list) != 5:
            return None, "Invalid number of arguments passed - expected 5"

        if len(data_list[1]) > 15:
            return None, "Username too long, 15 character limit!"

        # Check if the ports are already taken
        for port in data_list[3:]:
            if port in self.ports:
                return None, f"Port {port} already taken"
        
        err = self.valid_user(data_list[1])
        if err:
            return None, err

        # Add ports to the list of ports so that they are reserved
        self.ports.append(data_list[3])
        self.ports.append(data_list[4])

        user = self.User(data_list[1], data_list[2], data_list[3:])
        self.state_table[user.username] = user
        
        return f"{data_list[1]} added to state table successfully", None

    def deregister(self, data_list):
        '''
            This function will check if the user to deregister that was given from
            the client is valid and then removes the user from state_table state information

            ex: deregister ⟨username⟩
        '''
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2"
        
        if data_list[1] not in self.state_table.keys():
            return None, f"{data_list[1]} not in state table"
        
        user_to_deregister = self.state_table[data_list[1]]
        if user_to_deregister.state != 'Free':
            return None, "User given is not in Free state"
        else:
            self.state_table[user_to_deregister.username].registered = False
            del self.state_table[user_to_deregister.username]

        return "Successfully removed user from state table", None

    def setup_dht(self, data_list):
        '''
            Setup the local server DHT & three_tuples within the server
            Also updates the state_table state information

            ex: setup-dht ⟨n⟩ ⟨username⟩
        '''
        if len(data_list) != 3:
            return None, "Invalid number of arguments - expected 3"

        if data_list[2] not in self.state_table.keys():
            return None, f"{data_list[2]} not in state table"
        
        n = int(data_list[1])

        if n < 2 or n > len(self.state_table):
            return None, f"Invalid n value -> {n}"

        setup_dht_response = []
        self.dht_leader = data_list[2]

        # Start the id at 1 since the leader has id of 0
        dht_id = 1
        
        # Setting up the local State Table, server response message, and updating state_table
        for key, value in self.state_table.items():
            if key == data_list[2]:
                self.state_table[key].state = 'Leader'
                setup_dht_response.insert(0, {
                    'n': n,
                    'id': 0,
                    'username': value.username,
                    'ip': value.ipv4,
                    'port': value.client_port,
                    'query': value.client_query_port
                })
            elif value.state != 'InDHT' and dht_id != n:
                self.state_table[key].state = 'InDHT'
                setup_dht_response.append({
                    'n': n,
                    'id': dht_id,
                    'username': value.username,
                    'ip': value.ipv4,
                    'port': value.client_port,
                    'query': value.client_query_port
                })
                dht_id += 1    
        
        self.dht_flag = True
        self.creating_dht = True

        return setup_dht_response, None

    def valid_query(self, data_list):
        '''
            Checking if query command is valid and if it is then send response with information
            on a random user that the Client will use to initiate query search

            ex: query ⟨username⟩
        '''
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2."
        
        if not self.dht_flag:
            return None, "There is no DHT created"

        if data_list[1] not in self.state_table.keys():
            return None, f"{data_list[1]} is not registered with the server"

        if self.state_table[data_list[1]].state != 'Free':
            return None, f"{data_list[1]} is currently maintaining the DHT. Only free users can query the DHT."

        # All checks passed so this is a valid query command
        maintainers = ['Leader', 'InDHT']
        dht_maintainers = [user for user in self.state_table.values() if user.state in maintainers]
        random_user_index = random.randrange(len(dht_maintainers))
        random_user = dht_maintainers[random_user_index]
        random_user = {
            'username': random_user.username,
            'ip': random_user.ipv4,
            'query': random_user.client_query_port
        }

        return random_user, None

    def join_dht(self, data_list):
        '''
            Checking if join-dht command is valid and if it is then send response with information
            on the current leader of the DHT

            ex: join-dht ⟨username⟩
        '''
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2."

        if not self.dht_flag:
            return None, "There is no DHT created"

        join_data = {
                'username': None,
                'leader': None,
            }
        
        if data_list[1] not in self.state_table.keys():
            return None, f"{data_list[1]} is not registered with the server."

        if self.state_table[data_list[1]].state != 'Free':
            return None, f"{data_list[1]} is already maintaining the DHT."

        # Valid user given
        self.joining_user = data_list[1]
        self.stabilizing_dht = True
        join_data['username'] = data_list[1]
        leader = self.state_table[self.dht_leader]
        join_data['leader'] = [(leader.ipv4, leader.client_port), (leader.ipv4, leader.client_query_port)]

        return join_data, None
    
    def leave_dht(self, data_list):
        '''Simple check to see if the leave-dht command is valid'''
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2."

        if not self.dht_flag:
            return None, "There is no DHT created"
            
        maintainers = ['Leader', 'InDHT']
        dht_maintainers = [user for user in self.state_table.values() if user.state in maintainers]

        if (len(dht_maintainers)) < 2:
            return None, "Current DHT doesn't have enough maintainers for anyone to leave"

        if data_list[1] not in self.state_table.keys():
            return None, f"{data_list[1]} is not registered with the server."

        if self.state_table[data_list[1]].state == 'Free':
            return None, f"{data_list[1]} is not currently maintaining the DHT"
        
        # Valid user given
        self.leaving_user = data_list[1]
        self.stabilizing_dht = True
        return f"Removing {data_list[1]} from DHT", None

    def dht_rebuilt(self, data_list):
        '''Check to see if the rebuilt dht command is valid'''
        
        if self.leaving_user:
            if len(data_list) != 3:
                return None, "Invalid number of arguments - expected 3."

            if data_list[1] != self.leaving_user:
                return None, "Only the user who initiated the leave-dht can respond with complete"
            
            for user, value in self.state_table.items():
                if value.state == 'Leader':
                    value.state = 'InDHT'
                    self.state_table[user] = value
        
            self.state_table[self.leaving_user].state = 'Free'
            self.state_table[data_list[2]].state = 'Leader'

            self.dht_leader = data_list[2]
            self.stabilizing_dht = False
            self.leaving_user = None

            return "DHT has been successfully rebuilt", None

        elif self.joining_user:
            if len(data_list) != 2:
                return None, "Invalid number of arguments - expected 2."

            if data_list[1] != self.joining_user:
                return None, "Only the user who initiated the join-dht can respond with complete"
            
            # Updating the state of new DHT maintainer
            self.state_table[data_list[1]].state = 'InDHT'

            self.stabilizing_dht = False
            self.joining_user = None

            return "DHT has been successfully rebuilt", None
        
        else:
            return "There is no dht-rebuild in process", None
    
    def teardown_dht(self, data_list):
        '''Simple check to see if the teardown-dht command is valid'''
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2."

        if not self.dht_flag:
            return None, "There is no DHT created"

        if data_list[1] not in self.state_table.keys():
            return None, f"{data_list[2]} not in state table"
        
        if self.state_table[data_list[1]].state != 'Leader':
            return None, f"{data_list[1]} is not the leader of the DHT"

        # Set this flag so server knows it is in busy state
        self.tearing_down_dht = True
        return f"Initiating teardown of the DHT", None

    def teardown_complete(self, data_list):
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2."
        
        if data_list[1] != self.dht_leader:
            return None, "Only the DHT leader can send this command"
        
        if not self.tearing_down_dht and not self.stabilizing_dht:
            return None, "The DHT is not being torn down"
        
        self.reset_dht()
        return "Successfully destroyed DHT", None
    
    ############ Debugging ############
    def display_users(self):
        i = 1
        print("\nDisplaying all users in Server state table: ")
        for username, value in self.state_table.items():
            print(f"\n\t{i}:\t{username}\n\t")
            print(json.dumps(vars(value), sort_keys=False, indent=4))
            i += 1