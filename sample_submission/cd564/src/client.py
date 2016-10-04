import copy
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
                            'action': None,
                            'URL' : None}

    # Should be called immediately after constructor.
    def load_state(self):
        print 'Process %d alive for the first time' % self.id
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
        pass

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
        result = json.dumps(self, default = jdefault)
        return result

    # Called when self receives a message s from the master.
    # IS thread-safe.
    def receive_master(self, s):
        with self.lock:
            parts = s.split()
            # Begin three-phase commit.
            if parts[0] in ['add', 'delete']:
                if self.coordinator == self.id:
                    self.transaction = {'number' : self.transaction['number']+1,
                                        'song' : parts[1],
                                        'state' : 'uncertain',
                                        'action' : parts[0],
                                        'URL' : parts[2] if parts[0] == 'add' else None}
                    self.message = 'vote-req'
                    self.votes = {}
                    # Update live list for this transaction.
                    self.alive = self.broadcast()
                else:
                    self.send([-1], 'ack abort')
            elif parts[0] == 'crash':
                sys.exit(1)
            elif parts[0] in ['crashAfterAck',
                            'crashAfterVote',
                            'crashPartialCommit',
                            'crashPartialPreCommit',
                            'crashVoteREQ',
                            'vote NO']:
                self.flag = parts[0]
            # If we have the song
            elif parts[0] == 'get' and parts[1] in self.data:
                # Send song URL to master.
                url = self.data[parts[1]]
                self.send([-1], 'resp ' + url)
        print 'end receive_master'


    # Called when self receives message from another backend server.
    # IS thread-safe.
    def receive(self, s):
        with self.lock:
            m = json.loads(s)
            # Only pay attention to abort if in middle of transaction.
            if m['message'] == 'abort' and self.transaction['state'] not in ['aborted', 'committed']:
                self.transaction['state'] = 'abort'
            # Only pay attention to acks if you are the coordinator.
            if m['message'] == 'ack' and self.id == self.coordinator and self.transaction['state'] == 'precommitted':
                self.acks[m['id']] = True
                # All live processes have acked.
                if len(self.acks) == len(self.alive):
                    self.transaction['state'] = 'committed'
                    self.message = 'commit'
                    if m['transaction']['action'] == 'add':
                        self.data[m['transaction']['song']] = m['transaction']['URL']
                    else:
                        del self.data[m['transaction']['song']]
                    self.broadcast()
                    self.send([-1], 'ack commit')
            # Even the coordinator only updates data on receipt of commit.
            # Should only receive this emssage if internal state is precommitted.
            if m['message'] == 'commit' and self.transaction['state'] == 'precommitted':
                if m['transaction']['action'] == 'add':
                    self.data[m['transaction']['song']] = m['transaction']['URL']
                else:
                    del self.data[m['transaction']['song']]
            if m['message'] == 'precommit':
                self.message = 'ack'
                self.transaction['state'] = 'precommitted'
                self.send([self.coordinator], self.message_str())
                # TODO: what if coordinator is dead here?
            if m['message'] == 'state-req':
                self.message = 'state-resp'
                stuff = self.send([m['id']], self.message_str())
            if m['message'] == 'state-resp':
                # Self tried to learn state, didn't crash during a transaction.
                if self.transaction['state'] in ['committed', 'aborted']:
                    # Only update internal state if sender knows more than self.
                    if self.transaction['number'] <= m['transaction']['number'] and isinstance(m['coordinator'], (int,long)):
                        self.coordinator = m['coordinator']
                        self.data = m['data']
                        self.transaction = m['transaction']
                # Self crashed during a transaction.
                else:
                    # TODO: recovery code here.
                    pass
            if m['message'] == 'ur-elected':
                # TODO: termination protocol
                pass
            # Assume we only receive this correctly.
            if m['message'] == 'vote-req':
                self.transaction = m['transaction']
                self.message = 'vote-no' if self.flag == 'vote-no' else 'vote-yes'
                self.send([m['id']], self.message_str())
                # TODO: timeout actions.
            # Only accept no votes if you're the coordinator.
            if m['message'] == 'vote-no' and self.id == self.coordinator:
                self.message = 'abort'
                self.transaction['state'] = 'aborted'
                self.votes[m['id']] = False
                self.broadcast()
                # Tell master you've aborted.
                self.send(-1, 'ack abort')
            # Only accept yes votes if you're the coordinator
            if m['message'] == 'vote-yes' and self.id == self.coordinator:
                self.votes[m['id']] = True
                # Everybody has voted yes!
                if any(self.votes.values()):
                    self.acks = {}
                    self.message = 'precommit'
                    self.transaction['state'] = 'precommitted'
                    self.broadcast()
            # Whatever happened above, log it.
            self.log()
        print "end receive"

