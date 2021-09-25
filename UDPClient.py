from UDPServer import User
import json
import socket
import sys
import time

# ECHOMAX = 255 # Longest string to echo
# ITERATIONS = 5 # Number of iterations the client executes
HASH_SIZE = 353 # Size to initialize the local hash table to


def die_with_error(error_message):
    sys.exit(error_message)


def hash_pos(record):
    ascii_sum = 0
    for letter in record['Long Name']:
        ascii_sum += ord(letter)
    
    return ascii_sum % 353


def setup_local_dht():
    return [ [] for _ in range(HASH_SIZE) ]


def main(args):
    if len(args) < 3:
        die_with_error(f"Usage: {args[0]} <Server IP address> <Echo Port>\n")
    

    serv_IP = args[1]  # First arg: server IP address (dotted decimal)
    echo_serv_port = int(args[2])  # Second arg: Use given port
    user_dht = []


    print(f"client: Arguments passed: server IP {serv_IP}, port {echo_serv_port}\n")

    local_hash_table = []
    '''
    ABW,Aruba,Aruba,Aruba,AW,Aruban florin,Latin America & Caribbean,AW,2010
    AFG,Afghanistan,Afghanistan,Islamic State of Afghanistan,AF,Afghan afghani,South Asia,AF,1979
        local_hash_table example
        [
            [
                {
                    Country Code: ABW,
                    Short Name: Aruba,
                    Table Name: Aruba,
                    Long Name: ,
                    2-Alpha Code: ,
                    Currency Unit: ,
                    Region: ,
                    WB-2 Code: ,
                    Latest Population Census: 
                },
            ],
            [
                
            ]
            
        ]
    '''

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((serv_IP, echo_serv_port))

        # i = 0
        # print(f"client: Echoing strings for {ITERATIONS} iterations\n")
        
        local_hash_table = setup_local_dht()
        print("\nSuccessfully initialized local hash table\n")

        while True:            
            echo_string = input("\nEnter command for the server: ")

            if echo_string and echo_string != 'listen':
                print(f"\nClient: reads string ``{echo_string}''\n")
                echo_string = bytes(echo_string, 'utf-8')
                try:
                    s.sendall(echo_string)
                except:
                    die_with_error("client: sendall() error")
            elif echo_string != 'listen':
                die_with_error("client: error reading string to echo\n")
            else:
                print('Listening for server incoming data\n')

            
            data = s.recv(1024)
            data_decoded = data.decode('utf-8')
            print(f'data decoded: {data_decoded}')
            data_loaded = json.loads(data_decoded)
            print(data_loaded)
            
            if data_loaded['res'] == 'SUCCESS':
                print("client: received SUCCESS response from server")
                if data_loaded['data']:
                    print(f"\nclient: received data {data_loaded['data']} from server on IP address {serv_IP}\n")
                
                if data_loaded['type'] == 'DHT':
                    user_dht = data_loaded['data']
                elif data_loaded['type'] == 'record':
                    pos = hash_pos(data_loaded['data'])
                    id = pos % len(user_dht)
                    ports = user_dht[id][2]
                    if echo_serv_port in ports:
                        print("This is the desired location for record!")
                        local_hash_table[pos].append(data_loaded['data'])
                    else:
                        print("This is not the desired location for the record")
                        for i in range(len(user_dht) - 1):
                            if echo_serv_port in user_dht[i][2]:
                                addr = (user_dht[i+1][1], user_dht[i+1][2][0])
                                s.sendto(data, addr)
                                print("Sent data to next node")
                        
            else:
                die_with_error("client: recvfrom() failed")

            # if len(data.decode('utf-8')) > ECHOMAX:
            #     die_with_error("client: recvfrom() failed")
            
            # if from_address != serv_IP:
            #     die_with_error(f"client: Error: received a packet from unknown source: {from_address}.\n")

            


if __name__ == "__main__":
    main(sys.argv)