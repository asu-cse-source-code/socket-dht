import socket
import sys
import time


ECHOMAX = 255 # Longest string to echo

class User:
    def __init__(self, username, ip_address, ports):
        self.username = username
        self.ipv4 = ip_address
        self.ports = ports
        self.state = 'Free'


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
    if len(data_list) < 4:
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

    if not valid_user(data_list[2], users):
        print("\nInvalid user\n")
        return False, users, dht, None
    
    n = int(data_list[1])

    if n < 2 or n > len(users):
        print("\nn is not large enough a value\n")
        return False, users, dht, None

    # Remove 1 from n for the leader
    n -= 1

    leader = ()
    others = []
        
    for key, value in users.items():
        if key == data_list[2]:
            value.state = 'Leader'
            users[key] = value
            leader = (value.username, value.ipv4, value.port)

        elif value.state != 'InDHT':
            value.state = 'InDHT'
            users[key] = value
            others.append((value.username, value.ipv4, value.port))
            n -= 1
        
        if n == 0:
            break
    
    three_tuples = [leader]
    for user in others:
        three_tuples.append(user)
    
    return True, users, dht, three_tuples


def main(args, users):
    dht_flag = False # Set to true when a DHT has been setup
    dht = {}

    if len(args) != 2:
        die_with_error(f"Usage:  {args[0]} <UDP SERVER PORT>\n")
    

    echo_serv_port = int(args[1])  # First arg: Use given port

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("", echo_serv_port))
        except:
            die_with_error("server: bind() failed")
        

        while True:
        
            s.listen()

            print(f"server: Port server is listening to is: {echo_serv_port}\n")
            
            conn, addr = s.accept()

            with conn:
                print('Connected by', addr)
                while True:
                    data = conn.recv(1024)
                    
                    if data:
                        print(f"server: received string ``{data.decode('utf-8')}'' from client on IP address {addr[0]}\n")
                        data_list = data.decode('utf-8').split()
                        if data_list[0] == 'register':
                            if register(data_list, users):
                                user = User(data_list[1], data_list[2], data_list[3])
                                users[user.username] = user
                                conn.sendall(b'SUCCESS')
                            else:
                                conn.sendall(b'FAILURE')
                        elif data_list[0] == 'setup-dht':
                            # setup-dht ⟨n⟩ ⟨user-name⟩
                            if dht_flag:
                                conn.sendall(b'FAILURE')

                            # Make call to setup_dht    
                            valid, users, dht, three_tuples = setup_dht(data_list, users, dht)
                            print(three_tuples)
                            if valid:
                                conn.sendall(b'SUCCESS')
                            else:
                                conn.sendall(b'FAILURE')

                        else:
                            conn.sendall(data)

                    else:
                        break

                print("Disconnected")


if __name__ == "__main__":
    all_users = {}
    main(sys.argv, all_users)