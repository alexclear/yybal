#!/usr/bin/env python
from gevent.server import StreamServer
from gevent.http import HTTPServer
import array
import socket
import sys
import gevent.monkey
import errno
import re
from struct import *
from setproctitle import *

totalclientcommands = 0
totalnewconns = 0
totalclosedconns = 0
totalcomquery = 0
insert_pattern = re.compile(".*[Ii][Nn][Ss][Ee][Rr][Tt]")
select_pattern = re.compile("[^\(]*[Ss][Ee][Ll][Ee][Cc][Tt]")
totalselects = 0
totalinserts = 0

def do_handshake(client_socket, mysql_socket):
    data = mysql_socket.recv(4096)
    client_socket.sendall(data)
    data = client_socket.recv(4096)
    mysql_socket.sendall(data)
    data = mysql_socket.recv(4096)
    client_socket.sendall(data)

def do_commands(client_socket, mysql_socket):
    global totalclientcommands
    global totalclosedconns
    global totalcomquery
    global totalselects
    global totalinserts
    try:
        while True:
            command = client_socket.recv(4096)
#            print "Received from client, bytes: %(bytes)d, command byte: %(commandbyte)s, first byte: %(firstbyte)s" % { 'bytes': len(command), 'commandbyte': '{0:08b}'.format(ord(command[4])), 'firstbyte': '{0:08b}'.format(ord(command[0])) }
            totalclientcommands += 1
            mysql_socket.sendall(command)
            if ord(command[4]) == 1:
                mysql_socket.close()
                client_socket.close()
                break
            if ord(command[4]) == 3:
                totalcomquery += 1
                if insert_pattern.match(command):
                    totalinserts += 1
                elif select_pattern.match(command):
                    totalselects += 1
            while True:
                try:
                    recv_buffer = mysql_socket.recv(4096)
#                    print 'Received ' + str(len(recv_buffer)) + ' bytes from server'
#                    print 'Received from server: ' + recv_buffer
                    bytes = client_socket.send(recv_buffer)
#                    print str(bytes) + ' bytes sent to client'
                    if len(recv_buffer) == 0:
                        break
                    mysql_socket.setblocking(0)
                except socket.error as err:
                    if err.errno == errno.EWOULDBLOCK:
                        mysql_socket.setblocking(1)
                        break
                    else:
                        raise
        totalclosedconns += 1
    except:
        print "In except block"
        print "Unexpected error: ", sys.exc_info()[0]
        raise

def handle(client_socket, address):
    global totalnewconns
    totalnewconns += 1
    mysql_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)    
    mysql_socket.connect(('127.0.0.1', 3306))
    do_handshake(client_socket, mysql_socket)
    do_commands(client_socket, mysql_socket)

def stathandler(request):
    print "URI: " + request.uri
    if request.uri == '/stats':
        msg = 'Total commands: ' + str(totalclientcommands) + '\nTotal new conns: ' + str(totalnewconns) + \
              ', closed conns: ' + str(totalclosedconns) + '\n' + \
              'Total com_query: ' + str(totalcomquery) + ', total selects: ' + str(totalselects) + ', total inserts: ' + str(totalinserts)
        request.add_output_header('Connection', 'close')
        request.add_output_header('Content-type', 'text/plain')
        request.add_output_header('Content-length', str(len(msg)))
        request.send_reply(200, 'OK', msg)
    pass

setproctitle("yybal")
gevent.monkey.patch_socket()
statsserver = HTTPServer(('', 9080), stathandler)
statsserver.start()
server = StreamServer(('127.0.0.1', 33306), handle) # creates a new server
server.serve_forever() # start accepting new connections
