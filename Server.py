import json
import socket
import sys


class UDPServer:
    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def die_with_error(error_message):
        '''Function to kill the program and ouput the error message'''
        sys.exit(error_message)


    def send_response(self, addr, res, type, data=None):
        '''Function to send response from server to client to avoid repetition'''
        response_data = json.dumps({
                'res': res,
                'type': type,
                'data': data
            })

        self.socket.sendto(bytes(response_data, 'utf-8'), addr)