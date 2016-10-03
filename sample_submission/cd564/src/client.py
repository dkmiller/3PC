import json
import pickle
import sys
import threading

class Client:
    def __init__(self, pid, num_procs, send):
        # Array of processes that we think are alive.
        self.alive = [pid]
        # Unknown coordinator.
        self.coordinator = None
        # Internal hash table of URL : song_name.
        self.data = {}
        # Flag to crash at a certain moment, or to vote no. A process can have
        # at most one flag at a time. An additional flag will overwrite this
        # one.
        self.flag = None
        # Process id.
        self.id = pid
        # Single global lock.
        self.lock = threading.Lock()
        # Message to send. Possible values are:
        # abort, ack, commit, just-woke, state-resp, state-req, ur-elected,
        # vote-no, vote-req, vote-yes
        self.message = 'state-req'
        # Total number of processes (not including master).
        self.N = num_procs
        # Send functionL
        self.send = send
        # All known information about current transaction.
        self.transaction = {'number': 0,
                            'song' : None,
                            'state' : 'committed',
                            'type': None,
                            'URL' : None}

        # Find out current state (has self crashed, etc).
        with open('%dlog.p' % self.id, 'w+') as log:
            try:
                transaction = pickle.load(log)['state']
                # Previous transaction finished.
                if transaction['state'] in ['committed', 'aborted']:
                    self.broadcast()
                else:
                    # TODO: termination protocol.
                    pass
            # First time this process has started.
            except:
                # Find out who is alive and who is the coordinator.
                self.alive = self.broadcast()
                # Self is the first process to be started.
                if len(self.alive) == 1:
                    self.coordinator = self.id
                    # Tell master this process is now coordinator.
                    self.send([-1], 'coordinator %d' % self.id)

    # Broadcasts message corresponding to state and returns all live recipients.
    # The broadcast goes to all messages, including the sender.
    # NOT thread-safe.
    def broadcast(self):
        recipients = range(self.N) # PID of all processes.
        message = self.message_str() # Serialize self.
        return self.send(recipients, message)

    # Writes state to log.
    # NOT thread-safe.
    def log(self):
        # Overwrite old log.
        with open('%dlog.p' % self.id, 'w+') as log:
                pickle.dump(self, log)

    # Returns a serialized version of self's state. Since in this assignment,
    # an objects state will easily be captured in at most 300B, smaller than
    # the standard size of a TCP block, there is no cost incurred by including
    # possibly unnecessary information in every message.
    # NOT thread safe.
    def message_str(self):
        # Generic method for converting class to string
        def jdefault(o):
            if hasattr(o, '__dict__'):
                return o.__dict__
            else:
                return None
        return json.dumps(self, default = jdefault)

    # Called when self receives a message s from the master.
    # IS thread-safe.
    def receive_master(self, s):
        with self.lock:
            parts = s.split()
            # Begin three-phase commit.
            if parts[0] in ['add', 'delete'] and self.coordinator == self.id:
                self.transaction = {'number' : self.transaction['number']+1,
                                    'song' : parts[1],
                                    'state' : 'uncertain',
                                    'type' : parts[0],
                                    'URL' : parts[2] if parts[0] == 'add' else None}
                self.message = 'vote-req'
                self.votes = {}
                # Update live list for this transaction.
                self.alive = self.broadcast()
                if self.flag:
                    # TODO: this changes if there is a flag.
                    pass
            if parts[0] == 'crash':
                sys.exit(1)
            if parts[0] in ['crashAfterAck',
                            'crashAfterVote',
                            'crashPartialCommit',
                            'crashPartialPreCommit',
                            'crashVoteREQ',
                            'vote NO']:
                self.flag = parts[0]
            # If we have the song
            if parts[0] == 'get' and parts[1] in self.data:
                # Send song URL to master.
                self.send(-1, self.data[parts[1]])

    # Called when self receives message from another backend server.
    # IS thread-safe.
    def receive(self, s):
        with self.lock:
            m = Message(json.loads(s))
            # Only pay attention to abort if in middle of transaction.
            if m.message == 'abort' and self.transaction['state'] not in ['aborted', 'committed']:
                self.transaction['state'] = 'abort'
            # Only pay attention to acks if you are the coordinator.
            if m.message == 'ack' and self.id == self.coordinator and self.transaction['state'] == 'precommitted':
                self.acks[m.id] = True
                # All live processes have acked.
                if len(self.acks) == len(self.alive):
                    self.transaction['state'] = 'committed'
                    self.message = 'commit'
                    self.broadcast()
                    self.send([-1], 'ack commit')
            # Even the coordinator only updates data on receipt of commit.
            # Should only receive this emssage if internal state is precommitted.
            if m.message == 'commit' and self.transaction['state'] == 'precommitted':
                if m.transaction['action'] == 'add':
                    self.data[m.song] = m.URL
                elif m.transaction['action'] == 'delete':
                    del self.data[m.song]
            if m.message == 'pre-commit':
                self.message = 'ack'
                self.transaction['state'] = 'precommitted'
                self.send([self.coordinator], self.message_str())
                # TODO: what if coordinator is dead here?
            if m.message == 'state-req':
                self.message = 'state-resp'
                self.send([m.id], self.message())
            if m.message == 'state-resp':
                # Self tried to learn state, didn't crash during a transaction.
                if self.transaction['state'] in ['commited', 'aborted']:
                    # Only update internal state if sender knows more than self.
                    if self.transaction['number'] < m.transaction['number']:
                        self.data = m.data
                        self.transaction = m.transaction
                # Self crashed during a transaction.
                else:
                    # TODO: recovery code here.
                    pass
            if m.message == 'ur-elected':
                # TODO: termination protocol
                pass
            # Assume we only receive this correctly.
            if m.message == 'vote-req':
                self.transaction = m.transaction
                self.message = 'vote-no' if self.flag == 'vote-no' else 'vote-yes'
                self.send([m.id], self.message_str())
                # TODO: timeout actions.
            # Only accept no votes if you're the coordinator.
            if m.message == 'vote-no' and self.id == self.coordinator:
                self.message = 'abort'
                self.transaction['state'] = 'aborted'
                self.votes[m.id] = False
                self.broadcast()
                # Tell master you've aborted.
                self.send(-1, 'ack abort')
            # Only accept yes votes if you're the coordinator
            if m.message == 'vote-yes' and self.id == self.coordinator:
                self.votes[m.id] = True
                # Everybody has voted yes!
                if any(self.votes.values()):
                    self.acks = {}
                    self.message = 'precommit'
                    self.transaction['state'] = 'precommitted'
                    self.broadcast()
            # Whatever happened above, log it.
            self.log()


    # Wrapper class for the receipt of a json'd client.
    class Message(object):
        # Call on json.dumps(client, default=jdefault)
        def __init__(self, data):
            self.__dict__ = data

# Only used for debugging. TODO: delete before submission.
def test_send(pids, string):
    print 'sending %s to %s' % (string, str(pids))
    alive = []
    for p in pids:
        alive.append(p)
    return alive
