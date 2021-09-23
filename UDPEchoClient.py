import socket
import sys
import time

ECHOMAX = 255 # Longest string to echo
ITERATIONS = 5 # Number of iterations the client executes

def main(args):
    if len(args) < 3:
        print("Error")
        exit()
    


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

            if (echo_string):
                # echo_string[len(echo_string) - 1] = '\0'
                echo_string = bytes(echo_string, 'utf-8')
                print(f"\nClient: reads string ``{echo_string}''\n")
            else:
                exit()
            
            s.sendall(echo_string)
            
                

            # s.sendall(b'Hello, world')
            data = s.recv(1024)




if __name__ == "__main__":
    main(sys.argv)