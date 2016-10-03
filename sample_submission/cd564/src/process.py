from client import Client
import sys, os
import time
from socket import SOCK_STREAM, socket, AF_INET
from threading import Thread, Lock

client = 1
root_port = 20000
address = 'localhost'
processes = dict()
threads = dict()
conns = dict()
outgoing_conns = dict()

class ListenThread(Thread):
  def __init__(self, conn, addr):
    Thread.__init__(self)
    self.conn = conn
    self.addr = addr

  # From Daniel
  def run(self):
    global client
    while True:
      try:
        data = self.conn.recv(1024)
        data = data.split('\n')
        for line in data:
          client.receive(line)

      except:
        break

class WorkerThread(Thread):
  def __init__(self, address, internal_port, pid):
    Thread.__init__(self)
    self.sock = socket(AF_INET, SOCK_STREAM)
    self.sock.bind((address, internal_port))
    self.sock.listen(1)

  def run(self):
    global threads
    while True:
      conn, addr = self.sock.accept()
      handler = ListenThread(conn, addr)
      handler.start()

class ClientHandler(Thread):
  def __init__(self, index, address, port):
    Thread.__init__(self)
    self.index = index
    self.sock = socket(AF_INET, SOCK_STREAM)
    self.sock.connect((address, port))
    self.valid = True

    def run(self):
      while True:
        a = 1 # do something

    def send(self, msg):
      self.sock.send(str(msg) + '\n')

# master
class MasterHandler(Thread):
  def __init__(self, index, address, port):
    Thread.__init__(self)
    self.index = index
    self.sock = socket(AF_INET, SOCK_STREAM)
    self.sock.bind((address, port))
    self.sock.listen(1)
    self.conn, self.addr = self.sock.accept()
    self.valid = True

  def run(self):
    global client, conns, threads
    conns[-1] = self.conn
    while self.valid:
      try:
        data = self.conn.recv(1024)
        #print data
        data = data.split('\n')
        for line in data:
          client.receive_master(line)
        #sys.stderr.write(data)
#         line = data.split('\n')
#         for l in line:
#             s = l.split()
#             if len(s) < 2:
#                 continue
#             if s[0] == 'coordinator':
#                 leader_lock.acquire()
#                 leader = int(s[1])
#                 leader_lock.release()
#             elif s[0] == 'resp':
#                 sys.stdout.write(s[1] + '\n')
#                 sys.stdout.flush()
#                 wait_ack_lock.acquire()
#                 wait_ack = False
#                 wait_ack_lock.release()
#             elif s[0] == 'ack':
#                 wait_ack_lock.acquire()
#                 wait_ack = False
#                 wait_ack_lock.release()
      except:
        print sys.exc_info()
        self.valid = False
        del threads[self.index]
        self.sock.close()
        break

  def send(self, s):
    if self.valid:
      self.conn.send(str(s) + '\n')

  def close(self):
    try:
      self.valid = False
      self.sock.close()
    except:
      pass

def send(p_id, data):
  global root_port, outgoing_conns
  if p_id == -1:
    outgoing_conns[p_id].send(str(data) + '\n')
    return True

  try:
    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect((address, root_port + p_id))
    sock.send(str(data) + '\n')
    sock.close()
  except:
    return False
  return True

def main():
  global address, root_port, processes, outgoing_conns

  print sys.argv
  pid = int(sys.argv[1])
  num_processes = int(sys.argv[2])
  myport = int(sys.argv[3])

  # Connection with MASTER
  handler = MasterHandler(pid, address, myport)
  outgoing_conns[-1] = handler
  handler.start()

  # All incoming connections
  handler = WorkerThread(address, root_port+pid, pid)
  handler.start()

  # All outgoing connections
##  for pno in range(num_processes):
##    if pno == pid:
##      continue
##    handler = ClientHandler(pno, address, root_port+pno)
##    outgoing_conns[pno] = handler
##    handler.start()

  client = Client(pid, num_processes, send)

  while True:
    a = 1

  # append process information to file for all processes info
##  f = open('backend_servers', 'a')
##  f.write(str(pid) + "\n")
##  f.close()
##
##  # get information of all other processes
##  curr_processes = 0
##  while curr_processes != num_processes:
##    curr_processes = 0
##    f = open('backend_servers', 'r')
##    for line in f:
##      #line = line.split()
##      print line
##      curr_processes += 1
##    f.close()
##
##  while (True):
##    try:
##      in_data = processes[pid].recv(1024)
##      print in_data
##    except:
##      print "ErrorAsh" + sys.exc_info()
##      break

if __name__ == '__main__':
  main()
