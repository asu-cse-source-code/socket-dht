from server import UDPServer
from state import StateInfo
import sys


BUFFER_SIZE = 1024
    

def parse_data(server, state, data, address):
    '''
        This function will parse any messages sent to the server and call the corresponding functions
        If the command doesn't match any that the parser knows, responds with error
    '''
    if data:
        print(f"server: received string ``{data.decode('utf-8')}'' from client on ip: {address[0]} port {address[1]}\n")
        data_list = data.decode('utf-8').split()
        command = data_list[0]
        if state.creating_dht and command != 'dht-complete':
            server.send_response(addr=address, res='FAILURE', type='error', data='Creating DHT')
        elif state.stabilizing_dht and command != 'dht-rebuilt':
            server.send_response(addr=address, res='FAILURE', type='error', data='Stabilizing DHT')
        elif command == 'register':
            res, err = state.register(data_list)
            if err:
                server.send_response(addr=address, res='FAILURE', type='register-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='register', data=res)
        elif command == 'setup-dht':
            # setup-dht ⟨n⟩ ⟨user-name⟩
            if state.dht_flag:
                server.send_response(addr=address, res='FAILURE', type='setup-dht', data='DHT already created')
            else:
                # Make call to setup_dht
                res, err = state.setup_dht(data_list)
                if err:
                    server.send_response(addr=address, res='FAILURE', type='DHT-error', data=err)
                else:
                    server.send_response(addr=address, res='SUCCESS', type='DHT', data=res)
        elif command == 'deregister':
            res, err = state.deregister(data_list)
            if err:
                server.send_response(addr=address, res='FAILURE', type='deregister-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='deregister', data=res)
        elif command == 'query-dht':
            res, err = state.valid_query(data_list)
            if err:
                server.send_response(addr=address, res='FAILURE', type='query-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='query-response', data=res)
        elif command == 'dht-complete':
            if data_list[1] == state.dht_leader:
                if state.creating_dht:
                    state.creating_dht = False
                    server.send_response(addr=address, res='SUCCESS', type='dht-setup')
                else:
                    server.send_response(addr=address, res='FAILURE', type='dht-setup-error', data="DHT is not currently being created")
            else:
                server.send_response(addr=address, res='FAILURE', type='dht-setup-error', data=f"{state.dht_leader} is the DHT leader, not {data_list[1]}")
        elif command == 'join-dht':
            res, err = state.join_dht(data_list)
            if err:
                server.send_response(addr=address, res='FAILURE', type='join-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='join-response', data=res)
        elif command == 'leave-dht':
            res, err = state.leave_dht(data_list)
            if err:
                server.send_response(addr=address, res='FAILURE', type='leave-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='leave-response', data=res)
        elif command == 'dht-rebuilt':
            res, err = state.dht_rebuilt(data_list)
            if err:
                server.send_response(addr=address, res='FAILURE', type='rebuilt-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='rebuilt-response', data=res)
        elif command == 'teardown-dht':
            res, err = state.teardown_dht(data_list)
            if err:
                server.send_response(addr=address, res='FAILURE', type='teardown-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='teardown-response', data=res)
        elif command == 'teardown-complete':
            res, err = state.teardown_complete(data_list)
            if err:
                server.send_response(addr=address, res='FAILURE', type='teardown-complete-error', data=err)
            else:
                server.send_response(addr=address, res='SUCCESS', type='teardown-complete', data=res)
        elif command == 'display-users':
            state.display_users()
            server.send_response(addr=address, res='SUCCESS', type='debugging')
        elif command == 'display-dht':
            state.display_dht()
            server.send_response(addr=address, res='SUCCESS', type='debugging')
        else:
            server.send_response(addr=address, res='FAILURE', type='error', data='Unkown command')
    
    return server, state


def main(args):
    if len(args) != 2:
        sys.exit(f"Usage:  {args[0]} <UDP SERVER PORT>\n")
    

    server_port = int(args[1])  # First arg: Use given port

    server = UDPServer()
    state = StateInfo(server_port)

    try:
        server.socket.bind(("", server_port))
    except:
        server.die_with_error("server: bind() failed")
        
    print(f"server: Port server is listening to is: {server_port}\n")
    
    # Add loop here so that we can disconnect and reconnect to server
    while True:
        message, addr = server.socket.recvfrom(BUFFER_SIZE)

        server, state = parse_data(server, state, message, addr)


if __name__ == "__main__":
    main(sys.argv)