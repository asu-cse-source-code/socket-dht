'''
Developer: Austin Spencer
Class: CSE 434 Computer Networks
Professor: Syrotiuk
Due: 10/17/2021
Group: 85
Ports: 4300 - 43499

About:  Purpose of this project is to implement your own application program in which processes
    communicate using sockets to maintain a distributed hash table (DHT) dynamically, and 
    answer queries using it.

ClientDriver.py:
    - This file facilitates the clients and accepts user input to pass to client instance
    and interact with server

Usage:
    -- S = Server ; C = Client ; P = Port

    python ClientDriver.py ⟨username⟩ ⟨S IP⟩ ⟨S P⟩ ⟨C IP⟩ ⟨C left P⟩ ⟨C query P⟩ ⟨C accept P⟩
'''


from Client import Client
import sys
import time


HASH_SIZE = 353 # Size to initialize the local hash table to
BUFFER_SIZE = 4096 # Max bytes to take in
FILE_PATH = "StatsCountry.csv"
ALL_COMMANDS = ['leave-dht', 'join-dht', 'query-dht', 'deregister', 'teardown-dht', 'register', 'setup-dht']
DEBUGGING_COMMANDS = ['check-node', 'help', 'display-users']
BASIC_COMMANDS = ['leave-dht', 'join-dht', 'query-dht', 'deregister', 'teardown-dht']


def read_input(client):
    '''
All Commands:
    - register
        This command registers a new user with the server. All users must be registered prior to issuing other any other commands to the server.
    
    - setup-dht ⟨n⟩
        Where n ≥ 2 .
        This command initiates the construction of a DHT of size n, with user-name as its leader.
        Only one DHT may exist at one time.
    
    - query-dht
        This command is used to initiate a query of the DHT.
    
    - leave-dht
    
    - deregister
        This command removes the state of a this user from the state information base (if already Free), allowing it to terminate. 
    
    - join-dht
        This command will add this user to the existing DHT.

    - teardown-dht
        Where this user is the leader of the DHT. 
        This command initiates the deletion of the DHT.
Debugging Commands:
    - check-node
        This command while output important information about the current node

    - display-users
        These commands will do as they sound and have the server desplay the respective database
    '''
    while True:
        # Just a little delay for when you can enter next command
        time.sleep(0.2)
        user_input = input("\nEnter command for the server: ")
        data_list = user_input.split()
        command = data_list[0]

        if command in ALL_COMMANDS:
            string_to_serv = user_input
            if command in BASIC_COMMANDS:
                string_to_serv = f'{command} {client.user.username}'
            elif command == 'register':
                # register austin 127.0.0.1 64352 64330
                string_to_serv = f'{command} {client.user.username} {client.user.accept_port_address[0]} {client.user.accept_port_address[1]} {client.user.query_addr[1]}'
            elif command == 'setup-dht':
                if len(data_list) > 1:
                    string_to_serv = f'{command} {data_list[1]} {client.user.username}'
                else:
                    string_to_serv = f'{command}'
            # Send command to server
            string_to_serv = bytes(string_to_serv, 'utf-8')
            try:
                client.sockets.client_to_server.socket.sendto(string_to_serv, client.user.server_addr)
                client.listen()
            except Exception as error:
                print(error)
                print("client: sendall() error")
        elif command in DEBUGGING_COMMANDS:
            if command == 'check-node':
                client.output_node_info()
            elif command == 'help':
                print(read_input.__doc__)
            elif command == 'display-dht' or command == 'display-users':
                client.sockets.client_to_server.socket.sendto(bytes(command, 'utf-8'), client.user.server_addr)
        else:
            print("Invalid command! Send help if you need to see all valid commands.\n")


def main(args):
    '''
    Usage:
        -- S = Server ; C = Client ; P = Port

        python ClientDriver.py ⟨username⟩ ⟨S IP⟩ ⟨S P⟩ ⟨C IP⟩ ⟨C left P⟩ ⟨C query P⟩ ⟨C accept P⟩
    '''
    if not (len(args) == 8):
        print(f"Incorrect number of arguments\n\n", main.__doc__)
        quit()
    
    username = args[1]
    serv_IP = args[2]  # First arg: server IP address (dotted decimal)
    echo_serv_port = int(args[3])  # Second arg: Use given port
    client_IP = args[4]
    client_port = int(args[5])
    query_port = int(args[6])
    right_port = int(args[7])

    client = Client(username, serv_IP, echo_serv_port, client_IP, client_port, 
                    query_port, right_port, HASH_SIZE, BUFFER_SIZE, FILE_PATH)


    client.start_threads()

    read_input(client)


if __name__ == "__main__":
    main(sys.argv)