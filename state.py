import json
import random


class StateInfo:
    def __init__(self, port):
        self.users = {} # Initialize empty dictionary of users
        self.dht_flag = False
        self.creating_dht = False
        self.stabilizing_dht = False
        self.tearing_down_dht = False
        self.ports = [port]
        self.dht = []
        self.dht_leader = None
        self.three_tuples = ()
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
        for user in self.dht:
            user.state = 'Free'
            self.users[user.username] = user
        
        self.dht_flag = False
        self.tearing_down_dht = False
        self.dht = []
        self.dht_leader = None
        self.three_tuples = ()


    
    def valid_user(self, user):
        '''Helper function to check if the user given is valid for registry'''
        # Check if username already exists and also if the username is all alphabetical
        if user in self.users.keys():
            return "User already registered"

        if not user.isalpha():
            return "Username must be an alphabetic string"
        
        return None

    def register(self, data_list):
        '''
            This function will take in the command from client and check 
            if the given information is valid to register a new user

            ex: register austin ip port1 port2
        '''
        if len(data_list) != 5:
            return None, "Invalid number of arguments passed - expected 5"

        if len(data_list[1]) > 15:
            return None, "Username too long"

        # Check if the ports are already taken
        for port in data_list[3:]:
            if port in self.ports:
                return None, f"Port {port} already taken"
        
        err = self.valid_user(data_list[1])
        if err:
            return None, err

        user = self.User(data_list[1], data_list[2], data_list[3:])
        self.users[user.username] = user
        
        return f"{data_list[1]} added to state table successfully", None

    def deregister(self, data_list):
        '''
            This function will check if the user to deregister that was given from
            the client is valid and then removes the user from users state information

            ex: deregister austin
        '''
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2"
        
        user_to_deregister = self.users[data_list[1]]
        if user_to_deregister.state != 'Free':
            return None, "User given is not in Free state"
        else:
            self.users[user_to_deregister.username].registered = False
            del self.users[user_to_deregister.username]

        return "Successfully removed user from state table", None

    def setup_dht(self, data_list):
        '''
            Setup the local server DHT & three_tuples within the server
            Also updates the users state information

            ex: setup-dht 2 austin
        '''
        if len(data_list) != 3:
            return None, "Invalid number of arguments - expected 3"

        if data_list[2] not in self.users.keys():
            return None, f"{data_list[2]} not in state table"
        
        n = int(data_list[1])

        if n < 2 or n > len(self.users):
            return None, f"Invalid n value -> {n}"

        # Remove 1 from n for the leader
        dht_size = n
        n = 1

        leader = ()
        self.dht_leader = data_list[2]
        dht_leader = self.User
        dht_others = []
        others = []
        
        # Setting up the local DHT, three tuples, and updating users
        for key, value in self.users.items():
            if key == data_list[2]:
                value.state = 'Leader'
                self.users[key] = dht_leader = value
                leader = {
                    'n': dht_size,
                    'id': 0,
                    'username': value.username,
                    'ip': value.ipv4,
                    'port': value.client_port,
                    'query': value.client_query_port
                }

            elif value.state != 'InDHT' and n != dht_size:
                value.state = 'InDHT'
                self.users[key] = value
                others.append({
                    'n': dht_size,
                    'id': n,
                    'username': value.username,
                    'ip': value.ipv4,
                    'port': value.client_port,
                    'query': value.client_query_port
                })
                dht_others.append(value)
                n += 1
            
            if n == dht_size and len(leader) > 0:
                break
        
        self.three_tuples = [leader]
        self.dht = [dht_leader]
        for user in dht_others:
            self.dht.append(user)

        for user in others:
            self.three_tuples.append(user)
        
        self.dht_flag = True
        self.creating_dht = True

        return self.three_tuples, None


    def valid_query(self, data_list):
        '''Simple check to see if the query command is valid'''
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2."
        
        if not self.dht:
            return None, "There is no DHT created"

        for key, value in self.users.items():
            if key == data_list[1]:
                if value.state != 'Free':
                    return None, "User given doesn't have a state of Free"
                else:
                    # Valid user given
                    random_user_index = random.randrange(len(self.dht))
                    random_user = self.dht[random_user_index]
                    random_user = {
                        'username': random_user.username,
                        'ip': random_user.ipv4,
                        'query': random_user.client_query_port
                    }
                    return random_user, None

        return None, "Invalid user given"

    def join_dht(self, data_list):
        '''Simple check to see if the leave-dht command is valid'''
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2."

        if not self.dht:
            return None, "There is no DHT created"

        join_data = {
                'username': None,
                'leader': None,
            }
        
        for username, value in self.users.items():
            if username == data_list[1]:
                if value.state != 'Free':
                    return None, f"{data_list[1]} is already maintaining the DHT"
                else:
                    # Valid user given
                    self.joining_user = username
                    self.stabilizing_dht = True
                    join_data['username'] = value.username
            elif value.state == 'Leader':
                join_data['leader'] = [(value.ipv4, value.client_port), (value.ipv4, value.client_query_port)]
        
        if join_data['username']:
            return join_data, None

        return None, "Invalid user given"

    
    def leave_dht(self, data_list):
        '''Simple check to see if the leave-dht command is valid'''
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2."

        if not self.dht:
            return None, "There is no DHT created"

        if (len(self.dht)) < 2:
            return None, "Current DHT doesn't have enough maintainers for anyone to leave"

        for username, value in self.users.items():
            if username == data_list[1]:
                if value.state == 'Free':
                    return None, f"{data_list[1]} is not currently maintaining the DHT"
                else:
                    # Valid user given
                    self.leaving_user = username
                    self.stabilizing_dht = True
                    return f"Removing {username} from DHT", None

        return "Invalid user given"

    def dht_rebuilt(self, data_list):
        '''Check to see if the rebuilt dht command is valid'''
        
        if self.leaving_user:
            if len(data_list) != 3:
                return None, "Invalid number of arguments - expected 3."

            if data_list[1] != self.leaving_user:
                return None, "Only the user who initiated the leave-dht can respond with complete"
            
            for user, value in self.users.items():
                if value.state == 'Leader':
                    value.state = 'InDHT'
                    self.users[user] = value
        
            self.users[self.leaving_user].state = 'Free'
            self.users[data_list[2]].state = 'Leader'

            # Setting up the local DHT
            self.dht = []
            for _, value in self.users.items():
                if value.state == 'Leader':
                    self.dht.insert(0, value)
                elif value.state == 'InDHT':
                    self.dht.append(value)
                

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
            self.users[data_list[1]].state = 'InDHT'
            self.dht.append(self.users[data_list[1]])

            self.stabilizing_dht = False
            self.joining_user = None

            return "DHT has been successfully rebuilt", None
        
        else:
            return "There is no dht-rebuild in process", None
    
    def teardown_dht(self, data_list):
        '''Simple check to see if the teardown-dht command is valid'''
        if len(data_list) != 2:
            return None, "Invalid number of arguments - expected 2."

        if not self.dht:
            return None, "There is no DHT created"

        if data_list[1] not in self.users.keys():
            return None, f"{data_list[2]} not in state table"
        
        if self.users[data_list[1]].state != 'Leader':
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
    
    def display_users(self):
        i = 1
        print("\nDisplaying all users in Server DB: ")
        for username, value in self.users.items():
            print(f"\n\t{i}:\t{username}\n\t")
            print(json.dumps(vars(value), sort_keys=False, indent=4))
            i += 1

    def display_dht(self):
        i = 1
        print("\nDisplaying all dht members in Server DB: ")
        for user in self.dht:
            print(f"\n\t{i}:\t{user.username}\n\t")
            print(json.dumps(vars(user), sort_keys=False, indent=4))
            i += 1