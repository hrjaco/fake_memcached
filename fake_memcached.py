#!/usr/bin/env python

from gevent import socket
from gevent.server import StreamServer


d = {}


def handle_request(sock, address):
    print address
    fp = sock.makefile()
    while True:
        line = fp.readline()
        tokens = line.split(' ')
        if tokens[0] == 'get':
            key = tokens[1].strip()
            if key.find('|') != -1:
                key = key.split('|')[0]
            value = d[key]
            fp.write('VALUE %s 0 %d\r\n' % (key, len(value)))
            fp.write('%s\r\n' % value)
            fp.write('END\r\n')
            fp.flush()
        elif tokens[0] == 'set':
            print 'set'
            key = tokens[1]
            flags = int(tokens[2])
            exptime = int(tokens[3])
            bytes = int(tokens[4])
            # Do not support noreply.
            value = fp.readline().strip()
            # print 'fp.readline()', fp.readline()
            # assert fp.readline() == 'END\r\n'
            d[key] = value
            if len(tokens) == 6:
                # noreply
                pass
            else:
                fp.write('STORED\r\n')
                fp.flush()
        else:
            fp.write('CLIENT_ERROR\r\n')
            fp.flush()
    sock.shutdown(socket.SHUT_WR)
    sock.close()

server = StreamServer(
    ('localhost', 11211), handle_request)

server.serve_forever()