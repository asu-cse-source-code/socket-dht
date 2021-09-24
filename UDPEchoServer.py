import socket
import sys
import time


ECHOMAX = 255 # Longest string to echo

def die_with_error(error_message):
    sys.exit(error_message)


def main(args):
    if len(args) != 2:
        die_with_error(f"Usage:  {args[0]} <UDP SERVER PORT>\n")
    

    echo_serv_port = int(args[1])  # First arg: Use given port

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("", echo_serv_port))
        except:
            die_with_error("server: bind() failed")
        
        s.listen()

        print(f"server: Port server is listening to is: {echo_serv_port}\n")
        
        conn, addr = s.accept()

        with conn:
            print('Connected by', addr)
            while True:
                data = conn.recv(1024)
                
                if data:
                    # print(f'Received {data.decode("utf-8")}')
                    print(f"server: received string ``{data.decode('utf-8')}'' from client on IP address {addr[0]}\n")
                    conn.sendall(data)



if __name__ == "__main__":
    main(sys.argv)