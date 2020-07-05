#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A python class to replay Kongsberg .kmall files over unicast/multicast.
# Adapted from Giuseppe Masetti's HydrOffice hyo2_kng code.
#
# Lynette Davis, CCOM

import datetime
import getopt
import kmall
import logging
import numpy as np
import os
import socket
import struct
import sys
import threading

# __name__ is module's name
logger = logging.getLogger(__name__)


class KmallPlayer:

    def __init__(self, files: list, replay_timing: float = None, ip_out: str = "224.1.20.40",
                 port_out: int = 26103, port_in: int = 4001, unicast=False):
        self.files = files
        self._replay_timing = replay_timing
        self.port_in = port_in
        self.port_out = port_out
        self.ip_out = ip_out
        self.unicast = unicast

        self.sock_out = None
        self.sock_in = None

        self.dg_counter = None

    @property
    def replay_timing(self):
        return self._replay_timing

    @replay_timing.setter
    def replay_timing(self, value):
        self._replay_timing = value

    def _close_sockets(self):
        if self.sock_in:
            self.sock_in.close()
            self.sock_in = None
        if self.sock_out:
            self.sock_out.close()
            self.sock_out = None

    def init_sockets(self):
        """Initialize UDP sockets"""

        # TODO: I'm not sure if this is set up correctly for unicast vs multicast...
        # Unicast / Multicast:
        self.sock_out = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Multicast only:
        if not self.unicast:
            # Allow reuse of addresses
            self.sock_out.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # TODO: I think setting TTL to 1 makes this be unicast?
            # Set messages time-to-live to 1 to avoid forwarding beyond current network segment
            ttl = struct.pack('b', 1) # TODO: How does K-Controller do this?
            self.sock_out.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            # Set socket send buffer to size of UDP packet
            self.sock_out.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2 ** 16)

            logger.debug("sock_out > buffer %sKB" %
                         (self.sock_out.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF) / 1024))

    def send_datagrams(self, f, row, final_byteOffset):
        """
        Sends UDP datagrams extracted from kmall file.
        :param f: Opened, binary file to be read.
        :param row: Row of dataframe from indexed kmall file.
        """
        f.seek(row['ByteOffset'], 0)
        sent = False
        try:
            sent = self.sock_out.sendto(f.read(row['MessageSize']), (self.ip_out, self.port_out))
        except OSError as e:
            logger.warning("%s" % e)

        if sent:
            self.dg_counter += 1

        if row['ByteOffset'] == final_byteOffset:
            print("Datagrams transmitted: ", self.dg_counter)
            print("Closing file.")
            f.close()


    def interaction(self):
        """ Read and transmit datagrams """

        self.dg_counter = 0

        # scheduler = Scheduler()
        # scheduler.start()

        # TODO: For testing:
        nonMWCdgms = 0

        # Iterate over list of files:
        for fp in self.files:

            # Error checking for appropriate file types:
            fp_ext = os.path.splitext(fp)[-1].lower()
            if fp_ext not in [".kmall", ".kmwcd"]:
                logger.info("SIS 5 mode -> Skipping unsupported file extension: %s" % fp)
                continue

            # (From GM's code:)
            # try:
            #     f = open(fp, 'rb')
            #     f_sz = os.path.getsize(fp)
            # except (OSError, IOError):
            #     raise RuntimeError("Unable to open %s" % fp)

            # Index file (find offsets and sizes of each datagram):
            # Function index_file() creates a dataframe ("k.Index") containing fields for
            # "Time" (index), "ByteOffset","MessageSize", and "MessageType".
            k = kmall.kmall(fp)
            k.index_file()

            # Find #IIP and #IOP datagrams; capture timestamps (index).
            # We will want to send #IIP and #IOP datagrams first.
            IIP_index = None
            IOP_index = None
            for index, row in k.Index.iterrows():
                if IIP_index is None:
                    if '#IIP' in row['MessageType']:
                        IIP_index = index
                if IOP_index is None:
                    if '#IOP' in row['MessageType']:
                        IOP_index = index
                if IIP_index is not None and IOP_index is not None:
                    break

            # Sort k.Index by timestamp
            k.Index.sort_index(inplace=True)
            #print(k.Index)

            # ************************************************************
            # TODO: If I can figure out how to change index values, this won't be so annoying. Won't have to do these
            #  calculation when we have a set replay time...
            #if self._replay_timing is None: # Play datagrams in 'real-time'...
            # Calculate scheduled delay (earliest time is reference, with delay of zero).
            sched_delay = [x - k.Index.index[0] for x in k.Index.index]
            k.Index['ScheduledDelay'] = sched_delay
            # Reset scheduled delay for #IIP and #IOP datagrams (these will play immediately):
            k.Index.set_value(IIP_index, 'ScheduledDelay', -2)
            k.Index.set_value(IOP_index, 'ScheduledDelay', -1)

            # Sort k.Index by scheduled delay
            k.Index.sort_values(by=['ScheduledDelay'], inplace=True)

            #else: # Play datagrams at some fixed interval...
            if self._replay_timing is not None:
                #sched_delay = list(range((2 * self._replay_timing), (len(k.Index) * self._replay_timing), self._replay_timing))
                sched_delay = np.linspace(0, (len(k.Index) * self._replay_timing), len(k.Index), endpoint=False)
                k.Index['ScheduledDelay'] = sched_delay
                # Reset scheduled delay for #IIP and #IOP datagrams (these will play in the first 2 time steps):
                k.Index.set_value(IIP_index, 'ScheduledDelay', 0)
                k.Index.set_value(IOP_index, 'ScheduledDelay', self._replay_timing)

            # Sort k.Index by scheduled delay
            #k.Index.sort_values(by=['ScheduledDelay'], inplace=True)
            # ************************************************************

            # Testing
            #print(k.Index)

            f = open(fp, 'rb')
            #with open(fp, 'rb') as f:

            final_byteOffset = k.Index['ByteOffset'].iloc[-1]
            now = datetime.datetime.now()

            # Iterate through rows of sorted dataframe:
            for index, row in k.Index.iterrows():
                #if replay_timing is None: # Play datagrams in 'real-time'...


                # Send negative and zero delay datagrams immediately (#IIP, #IOP)
                # TODO: Handle big-ass MWC datagrams.
                if row['ScheduledDelay'] <= 0 and "#MWC" not in row['MessageType']:
                    # TODO: Testing:
                    nonMWCdgms += 1
                    self.send_datagrams(f, row, final_byteOffset)
                    if row['ScheduledDelay'] == 0:
                        now = datetime.datetime.now()
                # Schedule positive delay datagrams
                else:
                    # TODO: Handle MWC datagrams.
                    if "#MWC" not in row['MessageType']:
                        # TODO: Testing:
                        nonMWCdgms += 1
                        run_at = now + datetime.timedelta(seconds=row['ScheduledDelay'])
                        delay = (run_at - now).total_seconds()
                        threading.Timer(delay, self.send_datagrams, [f, row, final_byteOffset]).start()


                # else: # Play datagrams at some fixed interval...
                #     if "#MWC" not in row['MessageType']:
                #         now = datetime.datetime.now()
                #         run_at = now + datetime.timedelta(self._replay_timing)
                #         delay = (run_at - now).total_seconds()
                #         # TODO: Testing:
                #         nonMWCdgms += 1
                #         threading.Timer(replay_timing, self.send_datagrams, [f, row, final_byteOffset]).start()

            print("nonMWCdgms: ", nonMWCdgms)


    def run(self):
        logger.debug("kmall_player started -> in: %s, out: %s:%s, timing: %s"
                     % (self.port_in, self.ip_out, self.port_out, self._replay_timing))

        self.init_sockets()
        self.interaction()

        logger.debug("kmall_player ended")

    #def parse_command_line(self):


if __name__ == '__main__':

    # TODO: I'm not sure what all the default values should be:
    # Default values:
    # No default file name or directory
    file_m = None;
    # When replay_timing is set to None, file will replay at real-time speed
    replay_timing_m = None;
    # Default port_in, port_out, and ip_out based on G. Masseti's code
    port_in_m = 4001
    port_out_m = 26103
    ip_out_m = "224.1.20.40"
    # Multicast by default; set unicast to True for unicast
    unicast_m = False

    # Testing:
    # ip_out = "127.0.0.1" # For testing
    # # 2019 Thunder Bay - With Water Column
    # file = 'data/0019_20190511_204630_ASVBEN.kmall' # For testing

    # Read command line arguments for file/directory, replay_timing, ip_address, port_out, multicast/unicast
    try:
        opts, args = getopt.getopt(sys.argv[1:], "humi:p:f:t:", ["ip=", "port=", "file=", "timing="])
    except getopt.GetoptError:
        print("kmall_player.py")
        print("-f   <file_or_directory>")
        print("-t   <replay_timing_sec>")
        print("-i   <ip_address>")
        print("-p   <port>")
        print("-m   multicast")
        print("-u   unicast")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print("kmall_player.py")
            print("-f   <file_or_directory>")
            print("-t   <replay_timing_sec>")
            print("-i   <ip_address>")
            print("-p   <port>")
            print("-m   multicast")
            print("-u   unicast")
            sys.exit()
        elif opt in ('-f', '--file'):
            file_m = arg
        elif opt in ('-t', '--timing'):
            replay_timing_m = float(arg)
        elif opt in ('-i', '--ip'):
            ip_out_m = arg
        elif opt in ('-p', '--port'):
            port_out_m = int(arg)
        elif opt in ('-m', '--multicast'):
            unicast_m = False
        elif opt in ('-u', '--unicast'):
            unicast_m = True

    # Create/initialize new instance of KmallPlayer:
    player = KmallPlayer([file_m], replay_timing_m, ip_out_m, port_out_m, port_in_m, unicast_m)

    player.run()

