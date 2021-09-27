class StateInfo:
    def __init__(self, port):
        self.users = {} # Initialize empty dictionary of users
        self.dht_flag = False
        self.creating_dht = False
        self.ports = [port]
        self.dht = []
        self.three_tuples = ()

    class User:
        '''The User class will have as many instances as users registered'''
        def __init__(self, username, ip_address, ports):
            self.username = username
            self.ipv4 = ip_address
            # Convert port to integers
            self.client_port = int(ports[0])
            self.client_query_port = int(ports[1])
            self.state = 'Free'
            self.next = None
    
    def valid_user(self, user):
        '''Helper function to check if the user given is valid for registry'''
        # Check if username already exists and also if the username is all alphabetical
        if user in self.users.keys() or not user.isalpha():
            return False
        
        return True

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
        
        if not self.valid_user(data_list[1]):
            return None, "Invalid user"

        user = self.User(data_list[1], data_list[2], data_list[3:])
        self.users[user.username] = user
        
        return "User added successfully", None

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

        if self.valid_user(data_list[2]):
            return None, "Desired leader not in state table"
        
        n = int(data_list[1])

        if n < 2 or n > len(self.users):
            return None, f"Invalid n value -> {n}"

        # Remove 1 from n for the leader
        dht_size = n
        n = 1

        leader = ()
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
            # set the next to the following user
            self.dht[-1].next = user.username
            self.dht.append(user)

        for user in others:
            self.three_tuples.append(user)
        
        self.dht_flag = True
        self.creating_dht = True

        return self.three_tuples, None


    def valid_query(self, data_list):
        '''Simple check to see if the query command is valid'''
        if len(data_list) != 2:
            return "Invalid number of arguments - expected 2."

        for key, value in self.users.items():
            if key == data_list[1]:
                if value.state != 'Free':
                    return "User given doesn't have a state of Free"
                else:
                    # Valid user given
                    return None

        return "Invalid user given"