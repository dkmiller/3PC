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
my_pid = -2

timeout_wait = 40

class TimeoutThread(Thread):
  def __init__(self, timeout, waiting_on):
    Thread.__init__(self)
    self.timeout = timeout
    self.waiting_on = waiting_on
    self.__is__running = True
    self.__suspend__ = True

  def run(self):
    global timeout_wait
    while(self.__is__running):
      if not self.__suspend__:
        time.sleep(0.5)
        self.timeout -= 0.5

        if (self.timeout <= 0):
          if (self.waiting_on == 'coordinator-vote-req'):
            print 'timed out waiting for vote-req'
            # Run re-election protocol
            self.timeout = timeout_wait
          elif self.waiting_on == 'coordinator-precommit':
            print 'timed out waiting for precommit'
            # Run Termination protocol
            self.timeout = timeout_wait
          elif self.waiting_on == 'process-vote':
            print 'timed out waiting for votes'
            # Send abort to all
            self.__is__running = False
          elif self.waiting_on == 'process-acks':
            print 'timed out waiting for acks'
            # Send Commits to remaining processes
            self.__is__running = False
          elif self.waiting_on == 'coordinator-commit':
            print 'timed out waiting for commit'
            # Termination protocol
            self.__is__running = False
          self.suspend()

  def reset(self):
    global timeout_wait
    self.timeout = timeout_wait

  def restart(self):
    self.reset()
    self.__suspend__ = False

  def suspend(self):
    self.__suspend__ = True

  def stop(self):
    self.__is__running = False

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
        if data != "":
          data = data.split('\n')
          data = data[:-1]
          for line in data:
            #print "process - Inside loop - " + str(line)
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
    #conns[-1] = self.conn
    while True:
      try:
        data = self.conn.recv(1024)
        if data:
          data = data.split('\n')
          data = data[:-1]
          for line in data:
            client.receive_master(line)
      except:
        print sys.exc_info()
        #self.valid = False
        #del threads[self.index]
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

# deprecated
def send(p_id, data):

  global root_port, outgoing_conns, address
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

def send_many(p_id_list, data):
  print 'send_many being called by ' + str(p_id_list)
  global root_port, outgoing_conns, address, my_pid
  true_list = []
  for p_id in p_id_list:
    if p_id == -1:
      outgoing_conns[p_id].send(str(data) + '\n')
      true_list.append(p_id)
      continue
    #if p_id == my_pid:
    #  true_list.append(p_id)
    #  client.receive(data)
    #  continue

    try:
      sock = socket(AF_INET, SOCK_STREAM)
      sock.connect((address, root_port + p_id))
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
  my_pid = pid
  num_processes = int(sys.argv[2])
  myport = int(sys.argv[3])

  # Timeouts
  coordinator_timeout_vote_req = TimeoutThread(timeout_wait, 'coordinator-vote-req')
  coordinator_timeout_precommit = TimeoutThread(timeout_wait, 'coordinator-precommit')
  coordinator_timeout_commit = TimeoutThread(timeout_wait, 'coordinator-commit')
  process_timeout_vote = TimeoutThread(timeout_wait, 'process-vote')
  process_timeout_acks = TimeoutThread(timeout_wait, 'process-acks')
  coordinator_timeout_vote_req.start()
  coordinator_timeout_precommit.start()
  coordinator_timeout_commit.start()
  process_timeout_vote.start()
  process_timeout_acks.start()

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

  client = Client(pid, num_processes, send_many, coordinator_timeout_vote_req, coordinator_timeout_precommit, coordinator_timeout_commit, process_timeout_vote, process_timeout_acks)
  print "Client has been inited"
  client.load_state()
  mhandler.start()

  coordinator_timeout_vote_req.restart()

  while True:
    a = 1


if __name__ == '__main__':
  main()
