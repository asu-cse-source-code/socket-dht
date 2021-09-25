import json
import os
import socket
import sys
from _thread import *
import time


ECHOMAX = 255 # Longest string to echo
BUFFER_SIZE = 1024
dht_flag = False # Set to true when a DHT has been setup
creating_dht = False # Flag for when the dht is currently being created
dht = [] # This is our DHT for state info
users = {} # Initialize empty dictionary of users
thread_count = 0 # Initialize thread count to 0


class User:
    def __init__(self, username, ip_address, ports):
        self.username = username
        self.ipv4 = ip_address
        # Convert port to integers
        self.port = int(ports[0])
        self.client_port = None
        if len(ports) > 1:
            self.client_port = ports[1]
        self.state = 'Free'
        # self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client = None
        self.next = None
        self.registered = True


def iterate_users(users):
    for key, value in users.items():
        print(f"User {key} has values: {vars(value)}")

def die_with_error(error_message):
    sys.exit(error_message)


def valid_user(user, users):
    '''
    Helper function to check if the user given is valid for registry
    '''
    # Check if username already exists and also if the username is all alphabetical
    if user in users.keys() or not user.isalpha():
        return False
    
    return True


def register(data_list, users):
    if len(data_list) < 4 or len(data_list) > 6:
        print("\nInvalid number of arguments passed\n")
        return False

    if len(data_list[1]) > 15:
        print("Username too long")
        return False
    
    if not valid_user(data_list[1], users):
        print("\nInvalid user\n")
        return False

    return True


def deregister(data_list):
    global users

    if len(data_list) != 2:
        return False
    
    user_to_deregister = users[data_list[1]]
    if user_to_deregister.state != 'Free':
        return False
    else:
        del users[user_to_deregister.username]

    return True
    

def setup_dht(data_list, users, dht):
    if len(data_list) < 3:
        print("\nNot enough arguments passed\n")
        return False, users, dht, None

    if valid_user(data_list[2], users):
        print("\nLeader not in database\n")
        return False, users, dht, None
    
    n = int(data_list[1])

    if n < 2 or n > len(users):
        print("\nInvalid n value\n")
        return False, users, dht, None

    # Remove 1 from n for the leader
    n -= 1

    leader = ()
    dht_leader = User
    dht_others = []
    others = []
        
    for key, value in users.items():
        if key == data_list[2]:
            value.state = 'Leader'
            users[key] = dht_leader = value
            leader = (value.username, value.ipv4, value.port)

        elif value.state != 'InDHT':
            value.state = 'InDHT'
            users[key] = value
            others.append((value.username, value.ipv4, value.port))
            dht_others.append(value)
            n -= 1
        
        if n == 0:
            break
    
    three_tuples = [leader]
    new_dht = [dht_leader]
    for user in dht_others:
        # set the next to the following user
        new_dht[-1].next = user.username
        new_dht.append(user)

    for user in others:
        three_tuples.append(user)
    
    return True, users, new_dht, three_tuples


def setup_topology(dht):
    i = 0
    id = 0
    n = len(dht)
    for user in dht:
        if i+1 < n:
            i += 1
        else:
            i = 0
        response_data = json.dumps({
            'res': 'SUCCESS',
            'type': 'topology',
            'data': {
                'n': n,
                'id': id,
                'username': user.username,
                'ip': dht[i].ipv4,
                'port': dht[i].client_port,
            }
        })
        print(response_data)
        user.client.sendall(bytes(response_data, 'utf-8'))
        id += 1


def threaded_socket(user):
    global thread_count
    thread_count += 1
    print('Thread Number: ' + str(thread_count))
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((user.ipv4, user.port))
        except Exception as error:
            print(error)
            print(f"server: bind() failed for user: {user.username} ip: {user.ipv4} port: {user.port} ")
            return
    
        # Add loop here so that we can disconnect and reconnect to server
        while user.registered:
        
            sock.listen()

            print(f"server: Port server is listening to is: {user.client_port}\n")
            
            client, addr = sock.accept()

            print('Connected by', addr)

            user.client = client

            start_new_thread(threaded_client, (client, user.port ))
            
            thread_count += 1

            print('Thread Number: ' + str(thread_count))
        

def threaded_client(conn, port):
    
    with conn:
        # conn.send(str.encode('Welcome to the Servern'))
        global thread_count
        global dht
        global dht_flag
        global creating_dht
        global users
        while True:
            data = conn.recv(BUFFER_SIZE)

            if data:
                print(f"server: received string ``{data.decode('utf-8')}'' from client on port {port}\n")
                data_list = data.decode('utf-8').split()
                if data_list[0] == 'register':
                    if register(data_list, users):
                        user = User(data_list[1], data_list[2], data_list[3:])
                        users[user.username] = user
                        response_data = json.dumps({
                            'res': 'SUCCESS',
                            'type': 'register',
                            'data': None
                        })
                        
                        # for i in range(len(user.ports)):
                        start_new_thread(threaded_socket, (user, ))
                        
                    else:
                        response_data = json.dumps({
                            'res': 'FAILURE',
                            'data': None
                        })
                elif data_list[0] == 'setup-dht':
                    # setup-dht ⟨n⟩ ⟨user-name⟩
                    if dht_flag:
                        response_data = json.dumps({
                            'res': 'FAILURE',
                            'data': None
                        })
                    else:
                        # Make call to setup_dht    
                        valid, users, dht, three_tuples = setup_dht(data_list, users, dht)
                        if valid:
                            response_data = json.dumps({
                                'res': 'SUCCESS',
                                'type': 'DHT',
                                'data': three_tuples
                            })
                            dht_flag = True
                            creating_dht = True
                            setup_topology(dht)
                        else:
                            response_data = json.dumps({
                                'res': 'FAILURE',
                                'data': None
                            })
                elif data_list[0] == 'deregister':
                    if deregister(data_list):
                        response_data = json.dumps({
                                'res': 'SUCCESS',
                                'type': 'deregister',
                                'data': None
                            })
                    else:
                        response_data = json.dumps({
                            'res': 'FAILURE',
                            'data': None
                        })
                else:
                    response_data = json.dumps({
                            'res': 'SUCCESS',
                            'type': 'echo',
                            'data': data.decode('utf-8')
                        })

                # Send the servers response
                iterate_users(users)
                conn.sendall(bytes(response_data, 'utf-8'))
            else:
                break
                

def main(args):
    global thread_count

    if len(args) != 2:
        die_with_error(f"Usage:  {args[0]} <UDP SERVER PORT>\n")
    

    echo_serv_port = int(args[1])  # First arg: Use given port

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("", echo_serv_port))
        except:
            die_with_error("server: bind() failed")
        
        # Add loop here so that we can disconnect and reconnect to server
        while True:
        
            s.listen()

            print(f"server: Port server is listening to is: {echo_serv_port}\n")
            
            client, addr = s.accept()

            print('Connected by', addr)

            start_new_thread(threaded_client, (client,echo_serv_port, ))
            
            thread_count += 1

            print('Thread Number: ' + str(thread_count))
                    


if __name__ == "__main__":
    main(sys.argv)