import socket
import sys
import time

ECHOMAX = 255 # Longest string to echo
ITERATIONS = 5 # Number of iterations the client executes


def die_with_error(error_message):
    sys.exit(error_message)


def main(args):
    if len(args) < 3:
        die_with_error(f"Usage: {args[0]} <Server IP address> <Echo Port>\n")
    

    serv_IP = args[1]  # First arg: server IP address (dotted decimal)
    echo_serv_port = int(args[2])  # Second arg: Use given port


    print(f"client: Arguments passed: server IP {serv_IP}, port {echo_serv_port}\n")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((serv_IP, echo_serv_port))

        i = 0
        print(f"client: Echoing strings for {ITERATIONS} iterations\n")
        
        while i < ITERATIONS:
            i += 1
            
            echo_string = input("\nEnter string to echo: ")

            if echo_string:
                print(f"\nClient: reads string ``{echo_string}''\n")
                echo_string = bytes(echo_string, 'utf-8')
            else:
                die_with_error("client: error reading string to echo\n")
            
            try:
                s.sendall(echo_string)
            except:
                die_with_error("client: sendall() error")

            
            data = s.recv(1024)
            
            if data:
                print(f"client: received string ``{data.decode('utf-8')}'' from server on IP address \n")
            else:
                die_with_error("client: recvfrom() failed")

            if len(data.decode('utf-8')) > ECHOMAX:
                die_with_error("client: recvfrom() failed")
            
            # if from_address != serv_IP:
            #     die_with_error(f"client: Error: received a packet from unknown source: {from_address}.\n")

            


if __name__ == "__main__":
    main(sys.argv)