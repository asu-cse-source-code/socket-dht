from csv import DictReader
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
        # Convert ports to integers
        self.ports = [int(port) for port in ports if port.isdigit()]
        self.state = 'Free'
        # self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client = None
        self.next = None


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
    if len(data_list) < 4 or len(data_list) > 7:
        print("\nNot enough arguments passed\n")
        return False
    
    if not valid_user(data_list[1], users):
        print("\nInvalid user\n")
        return False

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
        print("\nn is not large enough a value\n")
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
            leader = (value.username, value.ipv4, value.ports)

        elif value.state != 'InDHT':
            value.state = 'InDHT'
            users[key] = value
            others.append((value.username, value.ipv4, value.ports))
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


def threaded_socket(user, i):
    global thread_count
    thread_count += 1
    print('Thread Number: ' + str(thread_count))
    
    if not i:
        i = 0
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((user.ipv4, user.ports[i]))
        except Exception as error:
            print(error)
            print(f"server: bind() failed for user: {user.username} ip: {user.ipv4} port: {user.ports[i]} ")
            return
    
        # Add loop here so that we can disconnect and reconnect to server
        while True:
        
            sock.listen()

            print(f"server: Port server is listening to is: {user.ports[i]}\n")
            
            client, addr = sock.accept()

            print('Connected by', addr)

            user.client = client

            # start_new_thread(threaded_client, (client, user.ports[i], user.socket ))
        
        


def threaded_client(conn, port, sock):
    
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
                        
                        for i in range(len(user.ports)):
                            start_new_thread(threaded_socket, (user,i, ))
                        
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
                        print(three_tuples)
                        if valid:
                            response_data = json.dumps({
                            'res': 'SUCCESS',
                            'type': 'DHT',
                            'data': three_tuples
                            })
                            dht_flag = True
                            creating_dht = True
                            setup_all_local_dht(dht, sock)
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


def setup_all_local_dht(dht, sock):
    with open(os.path.join(sys.path[0], "StatsCountry.csv"), "r") as data_file:
        csv_reader = DictReader(data_file)
        # Iterate over each row in the csv using reader object
        addr = (dht[0].ipv4, dht[0].ports[0])
        print(f"address: {addr}")
        for record in csv_reader:
            response_data = json.dumps({
                'res': 'SUCCESS',
                'type': 'record',
                'DHT': [vars(this_user) for this_user in dht],
                'data': record
            })
            
            # sock.sendto(bytes(response_data, 'utf-8'), addr)
            print(vars(dht[0]))
            dht[0].client.sendall(bytes(response_data, 'utf-8'))
                

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

            start_new_thread(threaded_client, (client,echo_serv_port,s, ))
            
            thread_count += 1

            print('Thread Number: ' + str(thread_count))
                    


if __name__ == "__main__":
    main(sys.argv)