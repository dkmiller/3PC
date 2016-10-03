import json
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
        print "Receiving internal msg: " + str(data)
        data = data.split('\n')
        data = data[:-1]
        for line in data:
          print "process - Inside loop - " + str(line)
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
        print "Receiving master msg: " + str(data)
        data = data.split('\n')
        data = data[:-1]
        for line in data:
          client.receive_master(line)
      except:
        print sys.exc_info()
        self.valid = False
        del threads[self.index]
        self.sock.close()
        break

  def send(self, s):
    if self.valid:
      print "Sending msg to master: " + str(s)
      self.conn.send(str(s) + '\n')

  def close(self):
    try:
      self.valid = False
      self.sock.close()
    except:
      pass

# deprecated
def send(p_id, data):
  global root_port, outgoing_conns, address
  if p_id == -1:
    outgoing_conns[p_id].send(str(data) + '\n')
    return True

  try:
    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect((address, root_port + p_id))
    print "Sending internal msg: " + str(data)
    sock.send(str(data) + '\n')
    sock.close()
  except:
    return False
  return True

def send_many(p_id_list, data):
  global root_port, outgoing_conns, address
  true_list = []
  for p_id in p_id_list:
    if p_id == -1:
      outgoing_conns[p_id].send(str(data) + '\n')
      true_list.append(p_id)
      continue

    try:
      sock = socket(AF_INET, SOCK_STREAM)
      sock.connect((address, root_port + p_id))
      print "Sending internal msg: " + str(data)
      sock.send(str(data) + '\n')
      sock.close()
    except:
      continue
    true_list.append(p_id)
  return true_list

def main():
  global address, client, root_port, processes, outgoing_conns

  print sys.argv
  pid = int(sys.argv[1])
  num_processes = int(sys.argv[2])
  myport = int(sys.argv[3])

  # Connection with MASTER
  mhandler = MasterHandler(pid, address, myport)
  outgoing_conns[-1] = mhandler

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

  client = Client(pid, num_processes, send_many)
  client.load_state()
  mhandler.start()

  print "Client has been inited"

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

##def jdefault(o):
##  return o.__dict__
##
### Wrapper class for the receipt of a json'd client.
##class Message(object):
##  # Call on json.dumps(client, default=jdefault)
##  def __init__(self, data):
##    self.__dict__ = data
##
##def test_send(pid, string):
##  print pid, string
##  return True
##
##class Client:
##  def __init__(self, pid, num_procs, send):
##    # Action that the current transaction will perform, e.g. 'get'.
##    self.action = None
##    # Set of processes that we think are alive.
##    self.alive = {pid : True}
##    # Unknown coordinator.
##    self.coordinator = None
##    # Internal hash table of URL : song_name.
##    self.data = {}
##    # Process id.
##    self.id = pid
##    # Total number of processes (not including master).
##    self.N = num_procs
##    # Send functionL
##    self.send = send
##    # Song title (to be added or deleted)
##    self.song = None
##    # Current state.
##    self.state = 'just-woke'
##    # Current transaction number.
##    self.transaction = 0
##    # Message recipient.
##    self.to = None
##    self.URL = None
##
##    # TODO: open log file here.
##
##    # Find out who is alive and who is the coordinator.
##    ##self.broadcast('just-woke')
##    self.broadcast()
##    # Self is the only live process.
##    if len(self.alive) == 1:
##      self.coordinator = self.id
##      # Tell master this process is now coordinator.
##      self.send(-1, 'coordinator %d' % self.id)
##
##    # Returns a serialized version of self's state.
##    def message(self):
##      return json.dumps(self, default = jdefault)
##
##    # Broadcasts message corresponding to state and internally updates alive
##    # list. The broadcast goes to all messages, including the sender.
##    def broadcast(self):
##      for p in xrange(self.N):
##        self.to = p
##        send_string = self.message()
##        # Process p received broadcast.
##        if self.send(p, send_string):
##          self.alive[p] = True
##        # Process p didn't receive broadcast.
##        elif p in self.alive:
##          del self.alive[p]
##        self.to = None
##
##    # Called when self receives a message s from the master.
##    def receive_master(self, s):
##      parts = s.split()
##      pid = int(parts[0])
##      # Begin three-phase commit.
##      if parts[1] == 'add' and self.coordinator == pid:
##        self.action = 'add'
##        self.song = parts[2]
##        self.state = 'vote-req'
##        # Start a new transaction.
##        self.transaction += 1
##        self.URL = parts[3]
##        self.votes = {}
##        self.broadcast()
##      if parts[1] == 'delete' and self.coordinator == pid:
##        self.action = 'delete'
##        self.song = parts[2]
##        self.state = 'vote-req'
##        self.transaction += 1
##        self.votes = {}
##        self.broadcast()
##      if parts[1] == 'get':
##        if parts[2] in self.data:
##          # Send song URL to master.
##          self.send(-1, self.data[parts[2]])
##
##    # Called when self receives message s from another backend server.
##    def receive(self, s):
##      m = Message(json.loads(s))
##      if m.state == 'abort':
##        self.state = 'abort'
##      # Only pay attention to acks if you are the coordinator.
##      if m.state == 'ack' and self.id == self.coordinator:
##        self.acks[m.id] = True
##        # All live processes have acked.
##        if len(self.acks) == len(self.alive):
##          self.state = 'commit'
##          self.broadcast()
##          self.send(-1, 'ack commit')
##      if m.state == 'commit':
##        if m.action == 'add':
##          self.data[m.song] = m.URL
##        elif m.action == 'delete':
##          del self.data[m.song]
##      if m.state == 'just-woke':
##        self.send(m.id, self.message())
##      if self.state == 'just-woke':
##        self.coordinator = msg.coordinator
##        self.transaction = msg.transaction
##      if m.state == 'pre-commit':
##        self.state = 'ack'
##        self.send(self.coordinator, self.message())
##      if m.state == 'ur-elected':
##        # TODO: termination protocol
##        pass
##      if m.state == 'vote-req':
##        self.action = m.action
##        # Increment transaction number if not coordinator.
##        if self.id != self.coordinator:
##          self.transaction += 1
##        self.song = m.song
##        self.state = 'vote-yes'
##        self.URL = m.URL
##        self.send(self.coordinator, self.message())
##      # Only accept no votes if you're the coordinator.
##      if m.state == 'vote-no' and self.id == self.coordinator:
##        self.state = 'abort'
##        self.votes[m.id] = False
##        self.broadcast()
##        # Tell master you've aborted.
##        self.send(-1, 'ack abort')
##      # Only accept yes votes if you're the coordinator
##      if m.state == 'vote-yes' and self.id == self.coordinator:
##        self.votes[m.id] = True
##        # Everybody vote yes!
##        if any(self.votes.values()):
##          self.acks = {}
##          self.state = 'pre-commit'
##          self.broadcast()
##

if __name__ == '__main__':
  main()
