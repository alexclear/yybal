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

nextconnindex = 0

def do_handshake(client_socket, mysql_sockets):
    data = ''
    ourdata = ''
    client_communication_finished = False
    for mysql_socket in mysql_sockets:
        data = mysql_socket["conn"].recv(4096)
#        print "Length of initial packet: %(len)d" % { 'len': len(data) }
#        print "data is: " + data
        firstnull = data.find("\x00", 4)
#        print "First null position is: " + str(firstnull)
        versize = firstnull - 5
        scramble2len = len(data) - versize - 36
        formatstr = "xxxxx" + str(versize) + "sxxxx8sxxxxxxxxBxxxxxxxxxx" + str(scramble2len) + "s"
#        print "The format string is: " + formatstr
        (version, scramble1, scramblelen, scramble2) = unpack(formatstr, data)
#        print "version is: " + version
#        print "scramblelen is: %(len)d" % { 'len': scramblelen }
#        print "scramble1 is: " + scramble1
#        print "scramble2 is: " + scramble2
        if not client_communication_finished:
            client_socket.sendall(data)
            data = client_socket.recv(4096)
#            print "Length of client packet: %(len)d" % { 'len': len(data) }
            firstnull = data.find("\x00", 36)
#            print "First null position is: " + str(firstnull)
#            print "data is: " + data
            usersize = firstnull - 36
            cliscramblelen = ord(data[firstnull+1])
#            print "Client scramble length is: %(len)d" % { 'len': cliscramblelen }
            dbnamestart = firstnull+1+cliscramblelen+1
#            print "A db name starts at: %(start)d" % { 'start': dbnamestart }
            dbnamelen = len(data) - dbnamestart
            formatstr = "3s33s" + str(usersize) + "sxB" + str(cliscramblelen) + "s" + str(dbnamelen) + "s"
            (packetlen, flags, username, cliscramblesize, cliscramble, dbname) = unpack(formatstr, data)
#            print "packetlen[0] is: %(byte)d" % { 'byte': ord(packetlen[0]) }
#            print "packetlen[1] is: %(byte)d" % { 'byte': ord(packetlen[1]) }
#            print "packetlen[2] is: %(byte)d" % { 'byte': ord(packetlen[2]) }
#            print "client scramble size is: %(size)d" % { 'size': cliscramblesize }
#            print "username is: " + username
#            print "client scramble is: " + cliscramble
#            print "dbname is: " + dbname
#            print "data is: " + data
#            if len(dbname) == 0:
#                print "Well, there is no DB name actually"
            plen = len(data) - 4 - cliscramblelen - usersize - 1 + len("croot\x00")
            ourdata = pack("BBB33s" + str(len("croot\x00")) + "sB" + str(dbnamelen) + "s", plen, 0, 0, flags, "croot\x00", 0, dbname)
#    print "Length of our client packet: %(len)d" % { 'len': len(ourdata) }
#    firstnull = ourdata.find("\x00", 36)
#    print "First null position is: " + str(firstnull)
#    print "ourdata is: " + ourdata
#    usersize = firstnull - 36
#    cliscramblelen = ord(ourdata[firstnull+1])
#    print "Client scramble length is: %(len)d" % { 'len': cliscramblelen }
#    dbnamestart = firstnull+1+cliscramblelen+1
#    print "A db name starts at: %(start)d" % { 'start': dbnamestart }
#    dbnamelen = len(ourdata) - dbnamestart
#    formatstr = "3s33s" + str(usersize) + "sxB" + str(cliscramblelen) + "s" + str(dbnamelen) + "s"
#    (packetlen, flags, username, cliscramblesize, cliscramble, dbname) = unpack(formatstr, ourdata)
#    print "packetlen[0] is: %(byte)d" % { 'byte': ord(packetlen[0]) }
#    print "packetlen[1] is: %(byte)d" % { 'byte': ord(packetlen[1]) }
#    print "packetlen[2] is: %(byte)d" % { 'byte': ord(packetlen[2]) }
#    print "client scramble size is: %(size)d" % { 'size': cliscramblesize }
#    print "username is: " + username
#    print "client scramble is: " + cliscramble
#    print "dbname is: " + dbname
#    print "ourdata is: " + ourdata
#    if len(dbname) == 0:
#        print "Well, there is no DB name actually"
        mysql_socket["conn"].sendall(ourdata)
        data = mysql_socket["conn"].recv(4096)
#        print "Length of answer packet: %(len)d" % { 'len': len(data) }
#        print "data: " + data
        if ord(data[4]) == 0:
            pass
#            print "Everything is OK"
        else:
            raise Exception("Can't connect to one of the MySQL servers")
        if not client_communication_finished:
            client_socket.sendall(data)
            client_communication_finished = True

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
    global nextconnindex
    try:
        while True:
            command = client_socket.recv(4096)
#            print "Received from client, bytes: %(bytes)d" % { 'bytes': len(command) }
#            print "Received from client, bytes: %(bytes)d, command byte: %(commandbyte)s, first byte: %(firstbyte)s" % { 'bytes': len(command), 'commandbyte': '{0:08b}'.format(ord(command[4])), 'firstbyte': '{0:08b}'.format(ord(command[0])) }
            totalclientcommands += 1
            start = datetime.now()
#            mysql_socket.sendall(command)
            recv_sockets = []
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
                    recv_sockets.append(mysql_sockets[0])
                elif select_pattern.match(command):
                    totalselects += 1
#                    mysql_sockets[0]["conn"].sendall(command)
#                    recv_socket = mysql_sockets[0]["conn"]
                    mysql_sockets[nextconnindex]["conn"].sendall(command)
                    recv_sockets.append(mysql_sockets[nextconnindex])
                    nextconnindex += 1
                    if nextconnindex == len(mysql_sockets):
                        nextconnindex = 0
                elif update_pattern.match(command):
                    totalupdates += 1
                    mysql_sockets[0]["conn"].sendall(command)
                    recv_sockets.append(mysql_sockets[0])
                elif delete_pattern.match(command):
                    totaldeletes += 1
                    mysql_sockets[0]["conn"].sendall(command)
                    recv_sockets.append(mysql_sockets[0])
                elif set_pattern.match(command):
                    totalsets += 1
                    # To all parties
                    for mysql_socket in mysql_sockets:
                        mysql_socket["conn"].sendall(command)
                        recv_sockets.append(mysql_socket)
                elif show_pattern.match(command):
                    totalshows += 1
                    mysql_sockets[0]["conn"].sendall(command)
                    recv_sockets.append(mysql_sockets[0])
                else:
                    raise Exception("We don't know this type of query: " + command)
            elif ord(command[4]) == 2:
                # This is "INIT_DB", send to all parties
                for mysql_socket in mysql_sockets:
                    mysql_socket["conn"].sendall(command)
                    recv_sockets.append(mysql_socket)
            elif ord(command[4]) == 0x1B:
                # This is "COM_SET_OPTION", send to all parties
                for mysql_socket in mysql_sockets:
                    mysql_socket["conn"].sendall(command)
                    recv_sockets.append(mysql_socket)
            else:
                raise Exception("We don't know this ord!")
            client_communication_finished = False
#            print "Number of recv_sockets: " + str(len(recv_sockets))
            for recv_socket in recv_sockets:
                while True:
                    try:
                        recv_buffer = recv_socket["conn"].recv(4096)
                        delta = datetime.now() - start
                        if delta.seconds > 1:
                            if delta.seconds > 10:
                                recv_socket["stats"]["ttlmore10s"] += 1
                            else:
                                recv_socket["stats"]["ttl1s10s"] += 1
                        else:
                            if delta.microseconds > 100000:
                                recv_socket["stats"]["ttl100ms1s"] += 1
                            elif delta.microseconds > 10000:
                                recv_socket["stats"]["ttl10ms100ms"] += 1
                            elif delta.microseconds > 1000:
                                recv_socket["stats"]["ttl1ms10ms"] += 1
                            else:
                                recv_socket["stats"]["ttlless1ms"] += 1
#                    print 'Received ' + str(len(recv_buffer)) + ' bytes from server'
#                    print 'Received from server: ' + recv_buffer
                        if not client_communication_finished:
                            bytes = client_socket.send(recv_buffer)
#                    print str(bytes) + ' bytes sent to client'
                        if len(recv_buffer) == 0:
                            break
                        recv_socket["conn"].setblocking(0)
                    except socket.error as err:
                        if err.errno == errno.EWOULDBLOCK:
                            recv_socket["conn"].setblocking(1)
                            break
                        else:
                            raise
                client_communication_finished = True
        totalclosedconns += 1
    except:
        print "In except block"
        print "Unexpected error: ", sys.exc_info()[0]
        raise

def handle(client_socket, address):
    global totalnewconns
    global mysql_stats
    totalnewconns += 1
    mysql_sockets = []
    # First connection is always master, it gets all modifications
    mysql_socket1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)    
    mysql_socket1.connect(('192.168.127.3', 3306))
    mysql_sockets.append({"conn": mysql_socket1, "stats":mysql_stats[0]})
    mysql_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)    
    mysql_socket2.connect(('192.168.127.4', 3306))
    mysql_sockets.append({"conn": mysql_socket2, "stats":mysql_stats[1]})
    do_handshake(client_socket, mysql_sockets)
    do_commands(client_socket, mysql_sockets)

def stathandler(request):
    global mysql_stats
    print "URI: " + request.uri
    if request.uri == '/stats':
        msg = 'Total commands: ' + str(totalclientcommands) + '\nTotal new conns: ' + str(totalnewconns) + \
              ', closed conns: ' + str(totalclosedconns) + '\n' + \
              'com_query: ' + str(totalcomquery) + ', selects: ' + str(totalselects) + \
              ', inserts: ' + str(totalinserts) + ', updates: ' + str(totalupdates) + ', deletes: ' + str(totaldeletes) + ', sets: ' + str(totalsets) + '\n'
        i=0
        for mysql_stat in mysql_stats:
            msg = msg + "\nServer #" + str(i) + ":\n" + \
                  't<1ms: ' + str(mysql_stat["ttlless1ms"]) + ', 1ms<t<10ms: ' + str(mysql_stat["ttl1ms10ms"]) + ', 10ms<t<100ms: ' + str(mysql_stat["ttl10ms100ms"]) + \
                  ', 100ms<t<1s: ' + str(mysql_stat["ttl100ms1s"]) + ', 1s<t<10s: ' + str(mysql_stat["ttl1s10s"]) + ', 10s<t: ' + str(mysql_stat["ttlmore10s"]) + '\n'
            i += 1
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
mysql_stats = []
mysql_stats.append({"numqueries":0, "ttlless1ms":0, "ttl1ms10ms":0, "ttl10ms100ms":0, "ttl100ms1s":0, "ttl1s10s":0, "ttlmore10s":0})
mysql_stats.append({"numqueries":0, "ttlless1ms":0, "ttl1ms10ms":0, "ttl10ms100ms":0, "ttl100ms1s":0, "ttl1s10s":0, "ttlmore10s":0})
thread.start_new_thread(process_stats, ())
gevent.monkey.patch_socket()
statsserver = HTTPServer(('', 9080), stathandler)
statsserver.start()
server = StreamServer(('127.0.0.1', 33306), handle) # creates a new server
server.serve_forever() # start accepting new connections
