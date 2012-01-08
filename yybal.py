#!/usr/bin/env python
from gevent.server import StreamServer
from gevent.http import HTTPServer
import array
import socket
import sys
import gevent.monkey
import errno
from datetime import *
import re
from struct import *
from setproctitle import *
import thread
import time

totalclientcommands = 0
totalnewconns = 0
totalclosedconns = 0
totalcomquery = 0
insert_pattern = re.compile(".*[Ii][Nn][Ss][Ee][Rr][Tt]")
select_pattern = re.compile("[^\(]*[Ss][Ee][Ll][Ee][Cc][Tt]")
update_pattern = re.compile(".*[Uu][Pp][Dd][Aa][Tt][Ee]")
set_pattern = re.compile(".*[Ss][Ee][Tt]")
show_pattern = re.compile(".*[Ss][Hh][Oo][Ww]")
delete_pattern = re.compile(".*[Dd][Ee][Ll][Ee][Tt][Ee]")
totalselects = 0
totalinserts = 0
totalupdates = 0
totalsets = 0
totalshows = 0
totaldeletes = 0

ttlless1ms = 0
ttl1ms10ms = 0
ttl10ms100ms = 0
ttl100ms1s = 0
ttl1s10s = 0
ttlmore10s = 0

nextconnindex = 0

def do_handshake(client_socket, mysql_sockets):
    data = ''
#    for mysql_socket in mysql_sockets:
    data = mysql_sockets[0]["conn"].recv(4096)
    client_socket.sendall(data)
    data = client_socket.recv(4096)
    mysql_sockets[0]["conn"].sendall(data)
    data = mysql_sockets[0]["conn"].recv(4096)
    client_socket.sendall(data)
#        break

def do_commands(client_socket, mysql_sockets):
    global totalclientcommands
    global totalclosedconns
    global totalcomquery
    global totalselects
    global totalinserts
    global totalupdates
    global totalsets
    global totalshows
    global totaldeletes
    global ttlless1ms
    global ttl1ms10ms
    global ttl10ms100ms
    global ttl100ms1s
    global ttl1s10s
    global ttlmore10s
    global nextconnindex
    try:
        while True:
            command = client_socket.recv(4096)
#            print "Received from client, bytes: %(bytes)d" % { 'bytes': len(command) }
#            print "Received from client, bytes: %(bytes)d, command byte: %(commandbyte)s, first byte: %(firstbyte)s" % { 'bytes': len(command), 'commandbyte': '{0:08b}'.format(ord(command[4])), 'firstbyte': '{0:08b}'.format(ord(command[0])) }
            totalclientcommands += 1
            start = datetime.now()
#            mysql_socket.sendall(command)
            recv_socket = ''
            if ord(command[4]) == 1:
                for mysql_socket in mysql_sockets:
                    mysql_socket["conn"].sendall(command)
                    mysql_socket["conn"].close()
                    client_socket.close()
                break
            elif ord(command[4]) == 3:
                totalcomquery += 1
                if insert_pattern.match(command):
                    totalinserts += 1
                    mysql_sockets[0]["conn"].sendall(command)
                    recv_socket = mysql_sockets[0]["conn"]
                elif select_pattern.match(command):
                    totalselects += 1
                    mysql_sockets[0]["conn"].sendall(command)
                    recv_socket = mysql_sockets[0]["conn"]
#                    mysql_sockets[nextconnindex]["conn"].sendall(command)
#                    recv_socket = mysql_sockets[nextconnindex]["conn"]
                    nextconnindex += 1
                    if nextconnindex == len(mysql_sockets):
                        nextconnindex = 0
                elif update_pattern.match(command):
                    totalupdates += 1
                    mysql_sockets[0]["conn"].sendall(command)
                    recv_socket = mysql_sockets[0]["conn"]
                elif delete_pattern.match(command):
                    totaldeletes += 1
                    mysql_sockets[0]["conn"].sendall(command)
                    recv_socket = mysql_sockets[0]["conn"]
                elif set_pattern.match(command):
                    totalsets += 1
                    # To all parties
                    mysql_sockets[0]["conn"].sendall(command)
                    recv_socket = mysql_sockets[0]["conn"]
                elif show_pattern.match(command):
                    totalshows += 1
                    mysql_sockets[0]["conn"].sendall(command)
                    recv_socket = mysql_sockets[0]["conn"]
                else:
                    raise Exception("We don't know this type of query: " + command)
            elif ord(command[4]) == 2:
                # This is "INIT_DB", send to all parties
                mysql_sockets[0]["conn"].sendall(command)
                recv_socket = mysql_sockets[0]["conn"]
            elif ord(command[4]) == 0x1B:
                # This is "COM_SET_OPTION", send to all parties
                mysql_sockets[0]["conn"].sendall(command)
                recv_socket = mysql_sockets[0]["conn"]
            else:
                raise Exception("We don't know this ord!")
            while True:
                try:
                    recv_buffer = recv_socket.recv(4096)
                    delta = datetime.now() - start
                    if delta.seconds > 1:
                        if delta.seconds > 10:
                            ttlmore10s += 1
                        else:
                            ttl1s10s += 1
                    else:
                        if delta.microseconds > 100000:
                            ttl100ms1s += 1
                        elif delta.microseconds > 10000:
                            ttl10ms100ms += 1
                        elif delta.microseconds > 1000:
                            ttl1ms10ms += 1
                        else:
                            ttlless1ms += 1
#                    print 'Received ' + str(len(recv_buffer)) + ' bytes from server'
#                    print 'Received from server: ' + recv_buffer
                    bytes = client_socket.send(recv_buffer)
#                    print str(bytes) + ' bytes sent to client'
                    if len(recv_buffer) == 0:
                        break
                    recv_socket.setblocking(0)
                except socket.error as err:
                    if err.errno == errno.EWOULDBLOCK:
                        recv_socket.setblocking(1)
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
    mysql_sockets = []
    # First connection is always master, it gets all modifications
    mysql_socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)    
    mysql_socket1.connect(('127.0.0.1', 3306))
#    mysql_socket1.connect(('192.168.127.3', 3306))
    mysql_sockets.append({"conn": mysql_socket1, "numqueries":0})
    mysql_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)    
    mysql_socket2.connect(('192.168.127.4', 3306))
    mysql_sockets.append({"conn": mysql_socket2, "numqueries":0})
    do_handshake(client_socket, mysql_sockets)
    do_commands(client_socket, mysql_sockets)

def stathandler(request):
    print "URI: " + request.uri
    if request.uri == '/stats':
        msg = 'Total commands: ' + str(totalclientcommands) + '\nTotal new conns: ' + str(totalnewconns) + \
              ', closed conns: ' + str(totalclosedconns) + '\n' + \
              'com_query: ' + str(totalcomquery) + ', selects: ' + str(totalselects) + \
              ', inserts: ' + str(totalinserts) + ', updates: ' + str(totalupdates) + ', deletes: ' + str(totaldeletes) + ', sets: ' + str(totalsets) + '\n' + \
              't<1ms: ' + str(ttlless1ms) + ', 1ms<t<10ms: ' + str(ttl1ms10ms) + ', 10ms<t<100ms: ' + str(ttl10ms100ms) + \
              ', 100ms<t<1s: ' + str(ttl100ms1s) + ', 1s<t<10s: ' + str(ttl1s10s) + ', 10s<t: ' + str(ttlmore10s)
        request.add_output_header('Connection', 'close')
        request.add_output_header('Content-type', 'text/plain')
        request.add_output_header('Content-length', str(len(msg)))
        request.send_reply(200, 'OK', msg)
    pass

def process_stats():
    print "This is a stats processing thread"
    while True:
        time.sleep(60)
        print "Let's process stats"

setproctitle("yybal")
thread.start_new_thread(process_stats, ())
gevent.monkey.patch_socket()
statsserver = HTTPServer(('', 9080), stathandler)
statsserver.start()
server = StreamServer(('127.0.0.1', 33306), handle) # creates a new server
server.serve_forever() # start accepting new connections
