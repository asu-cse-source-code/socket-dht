import socket
import sys
import time


serv_IP = '127.0.0.1'  # Standard loopback interface address (localhost)


def main(args):
    if len(args) != 2:
        print("Error")
        exit()
    


    echo_serv_port = int(args[1])  # First arg: Use given port


    print(f"server: Port server is listening to is: {echo_serv_port}\n")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((serv_IP, echo_serv_port))
        s.listen()
        conn, addr = s.accept()
        with conn:
            print('Connected by', addr)
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                conn.sendall(data)



if __name__ == "__main__":
    main(sys.argv)