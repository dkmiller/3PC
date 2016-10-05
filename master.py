#!/usr/bin/env python
"""
The master program for CS5414 three phase commit project.
"""

import sys, os
import subprocess
import time
from threading import Thread, Lock
from socket import SOCK_STREAM, socket, AF_INET

leader = -1 # coordinator
address = 'localhost'
threads = {}
live_list = {}
crash_later = []
wait_ack = False

class ClientHandler(Thread):
    def __init__(self, index, address, port):
        Thread.__init__(self)
        self.index = index
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect((address, port))
        self.buffer = ""
        self.valid = True

    def run(self):
        global leader, threads, wait_ack
        while self.valid:
            if "\n" in self.buffer:
			    #print self.buffer
                (l, rest) = self.buffer.split("\n",1)
                self.buffer = rest
                s = l.split()
                if len(s) < 2:
                    continue
                if s[0] == 'coordinator':
                    leader = int(s[1])
                    wait_ack = False
                elif s[0] == 'resp':
                    sys.stdout.write(s[1] + '\n')
                    sys.stdout.flush()
                    wait_ack = False
                elif s[0] == 'ack':
                    wait_ack = False
                else:
                    print s
            else:
                try:
                    data = self.sock.recv(1024)
                    #sys.stderr.write(data)
                    self.buffer += data
                except:
                    print sys.exc_info()
                    self.valid = False
                    del threads[self.index]
                    self.sock.close()
                    break

    def send(self, s):
        if self.valid:
            self.sock.send(str(s) + '\n')

    def close(self):
        try:
            self.valid = False
            self.sock.close()
        except:
            pass

def send(index, data, set_wait_ack=False):
    global leader, live_list, threads, wait_ack
    wait = wait_ack
    while wait:
        time.sleep(0.01)
        wait = wait_ack
    pid = int(index)
    if pid >= 0:
        if pid not in threads:
            print 'Master or testcase error!'
            return
        if set_wait_ack:
            wait_ack = True
        threads[pid].send(data)
        return
    pid = leader
    while pid not in live_list or live_list[pid] == False:
        time.sleep(0.01)
        pid = leader
    if set_wait_ack:
        wait_ack = True
    threads[pid].send(data)

def exit():
    global threads, wait_ack

    wait = wait_ack
    while wait:
        time.sleep(0.01)
        wait = wait_ack

    time.sleep(2)
    for k in threads:
        threads[k].close()
    subprocess.Popen(['./stopall'], stdout=open('/dev/null'), stderr=open('/dev/null'))
    time.sleep(0.1)
    os._exit(0)

def main():
    global leader, threads, crash_later, wait_ack
    while True:
        line = ''
        try:
            line = sys.stdin.readline()
        except: # keyboard exception, such as Ctrl+C/D
            exit()
        if line == '': # end of a file
            exit()
        line = line.strip() # remove trailing '\n'
        if line == 'exit': # exit when reading 'exit' command
            exit()
        sp1 = line.split(None, 1)
        sp2 = line.split()
        if len(sp1) != 2: # validate input
            continue
        pid = int(sp2[0]) # first field is pid
        cmd = sp2[1] # second field is command
        if cmd == 'start':
            port = int(sp2[3])
            # if no leader is assigned, set the first process as the leader
            if leader == -1:
                leader = pid
            live_list[pid] = True
            subprocess.Popen(['./process', str(pid), sp2[2], sp2[3]], stdout=open('/dev/null'), stderr=open('/dev/null'))
            # sleep for a while to allow the process be ready
            time.sleep(1)
            # connect to the port of the pid
            handler = ClientHandler(pid, address, port)
            threads[pid] = handler
            handler.start()
        elif cmd == 'get':
            send(pid, sp1[1], set_wait_ack=True)
        elif cmd == 'add' or cmd == 'delete':
            send(pid, sp1[1], set_wait_ack=True)
            for c in crash_later:
                live_list[c] = False
            crash_later = []
        elif cmd == 'crash':
            send(pid, sp1[1])
            if pid == -1:
                pid = leader
            live_list[pid] = False
        elif cmd[:5] == 'crash':
            send(pid, sp1[1])
            if pid == -1:
                pid = leader
            crash_later.append(pid)
        elif cmd == 'vote':
            send(pid, sp1[1])
        time.sleep(1)

if __name__ == '__main__':
    main()
