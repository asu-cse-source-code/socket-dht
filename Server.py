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
    - This script contains a simple class that initializes a UDP socket and has some methods within that
    are very useful for the client and the server. 

'''


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