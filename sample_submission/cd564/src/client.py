import json


def jdefault(o):
    return o.__dict__

# Wrapper class for the receipt of a json'd client.
class Message(object):
    # Call on json.dumps(client, default=jdefault)
    def __init__(self, data):
        self.__dict__ = data

def test_send(pid, string):
    print pid, string
    return True

class Client:
    def __init__(self, pid, num_procs, send):
        # Action that the current transaction will perform, e.g. 'get'.
        self.action = None
        # Set of processes that we think are alive.
        self.alive = {pid : True}
        # Unknown coordinator.
        self.coordinator = None
        # Internal hash table of URL : song_name.
        self.data = {}
        # Process id.
        self.id = pid
        # Total number of processes (not including master).
        self.N = num_procs
        # Send functionL
        self.send = send
        # Song title (to be added or deleted)
        self.song = None
        # Current state.
        self.state = 'just-woke'
        # Current transaction number.
        self.transaction = 0
        # Message recipient.
        self.to = None
        self.URL = None

        # TODO: open log file here.

        # Find out who is alive and who is the coordinator.
        self.broadcast('just-woke')
        # Self is the only live process.
        if len(self.alive) == 1:
            self.coordinator = self.id
            # Tell master this process is now coordinator.
            self.send(-1, 'coordinator %d' % self.id)

    # Returns a serialized version of self's state.
    def message(self):
        return json.dumps(self, default = jdefault)

    # Broadcasts message corresponding to state and internally updates alive
    # list. The broadcast goes to all messages, including the sender.
    def broadcast(self):
        for p in xrange(self.N):
            self.to = p
            send_string = self.message()
            # Process p received broadcast.
            if self.send(p, send_string):
                self.alive[p] = True
            # Process p didn't receive broadcast.
            elif p in self.alive:
                del self.alive[p]
            self.to = None

    # Called when self receives a message s from the master.
    def receive_master(self, s):
        parts = s.split()
        pid = int(parts[0])
        # Begin three-phase commit.
        if parts[1] == 'add' and self.coordinator == pid:
            self.action = 'add':
            self.song = parts[2]
            self.state = 'vote-req'
            # Start a new transaction.
            self.transaction += 1
            self.URL = parts[3]
            self.votes = {}
            self.broadcast()
        if parts[1] == 'delete' and self.coordinator == pid:
            self.action = 'delete'
            self.song = parts[2]
            self.state = 'vote-req'
            self.transaction += 1
            self.votes = {}
            self.broadcast()
        if parts[1] == 'get':
            if parts[2] in self.data:
                # Send song URL to master.
                self.send(-1, self.data[parts[2]])

    # Called when self receives message s from another backend server.
    def receive(self, s):
        m = Message(json.loads(s))
        if m.state == 'abort':
            self.state = 'abort'
        # Only pay attention to acks if you are the coordinator.
        if m.state == 'ack' and self.id == self.coordinator:
            self.acks[m.id] = True
            # All live processes have acked.
            if len(self.acks) == len(self.alive):
                self.state = 'commit'
                self.broadcast()
                self.send(-1, 'ack commit')
        if m.state == 'commit':
            if m.action == 'add':
                self.data[m.song] = m.URL
            elif m.action == 'delete':
                del self.data[m.song]
        if m.state == 'just-woke':
            self.send(m.id, self.message())
        if self.state == 'just-woke':
            self.coordinator = msg.coordinator
            self.transaction = msg.transaction
        if m.state == 'pre-commit':
            self.state = 'ack'
            self.send(self.coordinator, self.message())
        if m.state == 'ur-elected':
            # TODO: termination protocol
            pass
        if m.state == 'vote-req':
            self.action = m.action
            # Increment transaction number if not coordinator.
            if self.id != self.coordinator:
                self.transaction += 1
            self.song = m.song
            self.state = 'vote-yes'
            self.URL = m.URL
            self.send(self.coordinator, self.message())
        # Only accept no votes if you're the coordinator.
        if m.state == 'vote-no' and self.id = self.coordinator:
            self.state = 'abort'
            self.votes[m.id] = False
            self.broadcast()
            # Tell master you've aborted.
            self.send(-1, 'ack abort')
        # Only accept yes votes if you're the coordinator
        if m.state == 'vote-yes' and self.id = self.coordinator:
            self.votes[m.id] = True
            # Everybody vote yes!
            if any(self.votes.values()):
                self.acks = {}
                self.state = 'pre-commit'
                self.broadcast()
