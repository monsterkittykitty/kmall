#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A python class to test speed of receiving/writing (and publishing?) kmall datagrams.
#
# Lynette Davis, CCOM

import datetime
import socket
import struct

class KmallReceiver:
    def __init__(self):
        self.host = "127.0.0.1"
        self.port = 26103
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.host, self.port))

        self.count = 0
        self.file = None
        self.startTime = None
        self.endTime = None

    def read_size_and_type(self, data):
        format_to_unpack = "1I4s"
        fields = struct.unpack(format_to_unpack, data[0:struct.Struct(format_to_unpack).size])

        self.count += 1
        #print(self.count, ": ", fields[0], ", ", fields[1])

        return fields[0], fields[1]

    def run(self):
        while True:
            data, addr = self.socket.recvfrom(2 ** 16)
            #print(data)
            field1, field2 = self.read_size_and_type(data)
            # IIP datagram sends first!
            if b"IIP" in field2:
                print("IIP")
                self.startTime = datetime.datetime.now()
                if self.count > 1:
                    if not self.file.closed:
                        self.file.close()
                self.file = open(("/home/monster-kitty/Desktop/kmall/test_log_" + str(self.count)), 'wb')

            self.file.write(data)

            # if b"#MRZ" in field2:
            #     print("MRZ")

            if self.count > 4165:
                print(field1, ": ", field2)
                self.endTime = datetime.datetime.now()

                timeDelta = self.endTime - self.startTime
                seconds = timeDelta.total_seconds()
                print("Seconds: ", seconds)

if __name__ == '__main__':
    kmallReceiver = KmallReceiver()
    kmallReceiver.run()