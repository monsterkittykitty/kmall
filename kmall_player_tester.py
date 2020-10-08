#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import socket
import struct

class KmallPlayerTester:
    def __init__(self):

        self.UDP_IP = "127.0.0.1"
        #UDP_IP = "0.0.0.0"
        #UDP_IP = "192.168.60.174"
        #UDP_IP = "192.168.220.128"
        self.UDP_PORT = 26103

        self.sock = None

        self.count = 0

    def init_sockets(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.UDP_IP, self.UDP_PORT))

    def read_header_and_IIP(self):
        header = {}
        format_to_unpack = "1I4s2B1H2I3H1B"
        fields = struct.unpack(format_to_unpack, data[0:struct.Struct(format_to_unpack).size])

        # Datagram length in bytes. The length field at the start (4 bytes) and end
        # of the datagram (4 bytes) are included in the length count.
        header['numBytesDgm'] = fields[0]
        # Array of length 4. Multibeam datagram type definition, e.g. #AAA
        header['dgmType'] = fields[1]
        # Datagram version.
        header['dgmVersion'] = fields[2]
        # System ID. Parameter used for separating datagrams from different echosounders
        # if more than one system is connected to SIS/K-Controller.
        header['systemID'] = fields[3]
        # Echo sounder identity, e.g. 122, 302, 710, 712, 2040, 2045, 850.
        header['echoSounderID'] = fields[4]
        # UTC time in seconds + Nano seconds remainder. Epoch 1970-01-01.
        header['dgtime'] = fields[5] + fields[6] / 1.0E9
        header['dgdatetime'] = datetime.datetime.utcfromtimestamp(header['dgtime'])

        dg = {}
        dg['header'] = header

        # Size in bytes of body part struct. Used for denoting size of rest of the datagram.
        dg['numBytesCmnPart'] = fields[7]
        # Information. For future use.
        dg['info'] = fields[8]
        # Status. For future use.
        dg['status'] = fields[9]

        # Installation settings as text format. Parameters separated by ; and lines separated by , delimiter.
        tmp = data[26: dg['header']['numBytesDgm']]
        i_text = tmp.decode('UTF-8')
        dg['install_txt'] = i_text

        return dg

    def read_size_and_type(self, data):
        format_to_unpack = "1I4s"
        fields = struct.unpack(format_to_unpack, data[0:struct.Struct(format_to_unpack).size])

        self.count += 1
        print(self.count, ": ", fields[0], ", ", fields[1])
        # if fields[0] > (2 ** 16):
        #     print(data)
        #     exit()


if __name__ == '__main__':
    tester = KmallPlayerTester()
    tester.init_sockets()

    f = open("/home/monster-kitty/Desktop/kmall/newfile.kmall", 'wb')
    while True:
        data, addr = tester.sock.recvfrom(2 ** 16)
        # print(data)
        # print(read_header())
        f.write(data)
        tester.read_size_and_type(data)
