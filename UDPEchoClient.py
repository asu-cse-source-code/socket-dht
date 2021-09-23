import socket
import sys
import time



def main(args):
    if len(args) < 3:
        print("Error")
        exit()
    


    serv_IP = args[1]  # First arg: server IP address (dotted decimal)
    echo_serv_port = int(args[2])  # Second arg: Use given port


    print(f"client: Arguments passed: server IP {serv_IP}, port {echo_serv_port}\n")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((serv_IP, echo_serv_port))
        s.sendall(b'Hello, world')
        data = s.recv(1024)

    print(f'Received {repr(data)}')




if __name__ == "__main__":
    main(sys.argv)