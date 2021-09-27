import json
import random
import socket
import sys


BUFFER_SIZE = 1024

class User:
    '''The User class will have as many instances as users registered'''
    def __init__(self, username, ip_address, ports):
        self.username = username
        self.ipv4 = ip_address
        # Convert port to integers
        self.port = int(ports[0])
        self.client_port = self.client_query_port = None
        if len(ports) > 2:
            self.client_port = ports[1]
            self.client_query_port = ports[2]
        self.state = 'Free'
        self.next = None

class StateInfo:
    def __init__(self, port):
        self.users = {} # Initialize empty dictionary of users
        self.dht_flag = False
        self.creating_dht = False
        self.ports = [port]
        self.dht = []
        self.three_tuples = ()
    
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
        '''
        if len(data_list) < 4 or len(data_list) > 6:
            print("\nInvalid number of arguments passed\n")
            return False

        if len(data_list[1]) > 15:
            print("Username too long")
            return False

        # Check if the ports are already taken
        for port in data_list[3:]:
            if port in self.ports:
                print(f"\nPort {port} already taken\n")
                return False
        
        if not self.valid_user(data_list[1]):
            print("\nInvalid user\n")
            return False

        user = User(data_list[1], data_list[2], data_list[3:])
        self.users[user.username] = user
        
        return True

    def deregister(self, data_list):
        '''
            This function will check if the user to deregister that was given from
            the client is valid and then removes the user from users state information
        '''
        global users

        if len(data_list) != 2:
            return False
        
        user_to_deregister = self.users[data_list[1]]
        if user_to_deregister.state != 'Free':
            return False
        else:
            self.users[user_to_deregister.username].registered = False
            del self.users[user_to_deregister.username]

        return True

    def setup_dht(self, data_list):
        '''
            Setup the local server DHT & three_tuples within the server
            Also updates the users state information
        '''
        if len(data_list) < 3:
            print("\nNot enough arguments passed\n")
            return False

        if self.valid_user(data_list[2]):
            print("\nLeader not in database\n")
            return False
        
        n = int(data_list[1])

        if n < 2 or n > len(self.users):
            print("\nInvalid n value\n")
            return False

        # Remove 1 from n for the leader
        dht_size = n
        n = 1

        leader = ()
        dht_leader = User
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

        return True


    def valid_query(self, data_list):
        '''Simple check to see if the query command is valid'''
        for key, value in self.users.items():
            if key == data_list[1]:
                return value.state == 'Free'

        return False


class UDPServer:
    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def die_with_error(error_message):
        '''Function to kill the program and ouput the error message'''
        sys.exit(error_message)


    def send_response(self, addr, res, type, data=None):
        '''Function to send response from server to client to avoid repetition'''
        response_data = json.dumps({
                'res': res,
                'type': type,
                'data': data
            })

        self.socket.sendto(bytes(response_data, 'utf-8'), addr)

    

def parse_data(server, state, data, address):
    '''
        This function will parse any messages sent to the server and call the corresponding functions
        If the command doesn't match any that the parser knows, responds with error
    '''
    if data:
        print(f"server: received string ``{data.decode('utf-8')}'' from client on ip: {address[0]} port {address[1]}\n")
        data_list = data.decode('utf-8').split()
        command = data_list[0]
        if state.creating_dht and command != 'dht-complete':
            server.send_response(addr=address, res='FAILURE', type='error', data='Creating DHT')
        elif command == 'register':
            if state.register(data_list):
                server.send_response(addr=address, res='SUCCESS', type='register')
            else:
                server.send_response(addr=address, res='FAILURE', type='register-error')
        elif command == 'setup-dht':
            # setup-dht ⟨n⟩ ⟨user-name⟩
            if state.dht_flag:
                server.send_response(addr=address, res='FAILURE', type='setup-dht', data='DHT already created')
            else:
                # Make call to setup_dht
                if state.setup_dht(data_list):
                    server.send_response(addr=address, res='SUCCESS', type='DHT', data=state.three_tuples)
                else:
                    server.send_response(addr=address, res='FAILURE', type='DHT-error')
        elif command == 'deregister':
            if state.deregister(data_list):
                server.send_response(addr=address, res='SUCCESS', type='deregister')
            else:
                server.send_response(addr=address, res='FAILURE', type='deregister-error')
        elif command == 'query-dht':
            if state.valid_query(data_list):
                random_user_index = random.randrange(len(state.three_tuples))
                random_user = state.three_tuples[random_user_index]
                server.send_response(addr=address, res='SUCCESS', type='query-response', data=random_user)
            else:
                server.send_response(addr=address, res='FAILURE', type='query-error')
        elif command == 'dht-complete':
            if state.creating_dht and data_list[1] == state.dht[0].username:
                state.creating_dht = False
                server.send_response(addr=address, res='SUCCESS', type='dht-setup')
            else:
                server.send_response(addr=address, res='FAILURE', type='dht-setup-error')
        else:
            server.send_response(addr=address, res='FAILURE', type='error', data='Unkown command')
    
    return server, state


def main(args):
    if len(args) != 2:
        sys.exit(f"Usage:  {args[0]} <UDP SERVER PORT>\n")
    

    server_port = int(args[1])  # First arg: Use given port

    server = UDPServer()
    state = StateInfo(server_port)

    try:
        server.socket.bind(("", server_port))
    except:
        server.die_with_error("server: bind() failed")
        
    print(f"server: Port server is listening to is: {server_port}\n")
    
    # Add loop here so that we can disconnect and reconnect to server
    while True:
        message, addr = server.socket.recvfrom(BUFFER_SIZE)

        server, state = parse_data(server, state, message, addr)


if __name__ == "__main__":
    main(sys.argv)