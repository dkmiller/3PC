import copy
import json
import os
import pickle
import threading

class Client:
    def __init__(self, pid, num_procs, send, c_t_vote_req, c_t_prec, c_t_c, p_t_vote, p_t_acks):
        # Array of processes that we think are alive.
        self.alive = [pid]
        # True if a process restarts
        self.stupid = False
        # Unknown coordinator.
        self.coordinator = None
        # Internal hash table of URL : song_name.
        self.data = {}
        # Flags to crash at a certain moment, or to vote no.
        self.flags = {}
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
        self.num_messages_received_for_election = 0

        # Timeouts
        self.c_t_vote_req = c_t_vote_req
        self.c_t_prec = c_t_prec
        self.c_t_c = c_t_c
        self.p_t_vote = p_t_vote
        self.p_t_acks = p_t_acks
        # Alive list for re-election protocol
        self.election_alive_list = [pid]
        # Wait for a vote-req
        self.c_t_vote_req.restart()

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

    def re_election_protocol(self):
        with self.lock:
            print 'starting re-election'
            self.num_messages_received_for_election = 0
            self.message = 'lets-elect-coordinator'
            self.election_alive_list = self.broadcast()

            # If alive item not present in election_alive, remove from alive
            for p in self.alive:
                if p not in self.election_alive_list:
                    self.alive.remove(p)

    def after_timed_out_on_acks(self):
        with self.lock:
            self.message = 'commit'
            self.send(self.alive, self.message_str())
            self.log()

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
        to_send = {'alive' : self.alive,
                   'coordinator' : self.coordinator,
                   'data' : self.data,
                   'flags' : self.flags,
                   'id' : self.id,
                   'message' : self.message,
                   'transaction' : self.transaction}
        result = json.dumps(to_send)
        return result

    # Called when self receives a message s from the master.
    # IS thread-safe.
    def receive_master(self, s):
        print 'received from master: ' + str(s)
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
                    self.acks = {}
                    self.votes = {}
                    if 'crashVoteREQ' in self.flags:
                        # If this code executes, the coordinator will crash.
                        self.send(self.flags['crashVoteREQ'], self.message_str())
                        del self.flags['crashVoteREQ']
                        self.log()
                        os._exit(1)
                    else:
                        # Alive = set of processes that received the vote request.
                        self.alive = self.broadcast()
                        print 'self alive = ' + str(self.alive)
                        self.log()
                        # Wait for votes
                        self.p_t_vote.restart()
                else:
                    self.send([-1], 'ack abort')
            elif parts[0] == 'crash':
                os._exit(1)
            elif parts[0] in ['crashAfterAck', 'crashAfterVote']:
                self.flags[parts[0]] = True
            elif parts[0] in ['crashPartialCommit',
                              'crashPartialPreCommit',
                              'crashVoteREQ']:
                # Flag maps to list of process id's.
                self.flags[parts[0]] = map(int, parts[1:])
            # If we have the song
            elif parts[0] == 'get' and parts[1] in self.data:
                # Send song URL to master.
                url = self.data[parts[1]]
                self.send([-1], 'resp ' + url)
            elif parts[0] == 'vote' and parts[1] == 'NO':
                self.flags['vote NO'] = True
        print 'end receive_master'

    # Called when self receives message from another backend server.
    # IS thread-safe.
    def receive(self, s):
        print 'receive ' + s
        with self.lock:
            m = json.loads(s)
            # Only pay attention to abort if in middle of transaction.
            if m['message'] == 'abort' and self.transaction['state'] not in ['aborted', 'committed']:
                self.transaction['state'] = 'abort'
                self.log()
                # No longer need to wait for precommit.
                self.c_t_prec.suspend()
                # Wait for next vote request.
                self.c_t_vote_req.restart()
            # Only pay attention to acks if you are the coordinator.
            if m['message'] == 'ack' and self.id == self.coordinator and self.transaction['state'] == 'precommitted':
                self.acks[m['id']] = True
                # All live processes have acked.
                if len(self.acks) == len(self.alive):
                    # Stop waiting for acks
                    self.p_t_acks.suspend()
                    self.message = 'commit'
                    self.log()
                    if 'crashPartialCommit' in self.flags:
                        self.send(self.flags['crashPartialCommit'], self.message_str())
                        del self.flags['crashPartialCommit']
                        os._exit(1)
                    else:
                        self.send(self.alive, self.message_str())
                    self.send([-1], 'ack commit')
            # Even the coordinator only updates data on receipt of commit.
            # Should only receive this emssage if internal state is precommitted.
            if m['message'] == 'commit' and self.transaction['state'] == 'precommitted':
                # No longer wait for commit.
                self.c_t_c.suspend()
                if m['transaction']['action'] == 'add':
                    self.data[m['transaction']['song']] = m['transaction']['URL']
                else:
                    del self.data[m['transaction']['song']]
                self.log()
                # Wait for next vote request.
                self.c_t_vote_req.restart()
            if m['message'] == 'precommit':
                # No longer need to wait for precommit.
                self.c_t_prec.suspend()
                self.message = 'ack'
                self.transaction['state'] = 'precommitted'
                self.send([self.coordinator], self.message_str())
                self.log()
                if 'crashAfterAck' in self.flags:
                    del self.flags['crashAfterAck']
                    os._exit(1)
                # Wait for commit.
                self.c_t_c.restart()
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
                print 'received vote-req'
                # Restart timeout counter
                self.c_t_vote_req.suspend()
                self.transaction = m['transaction']
                #self.alive = m['alive']
                if 'vote NO' in self.flags:
                    self.message = 'vote-no'
                    self.transaction['state'] = 'aborted'
                    # Clear your flag for the next transaction.
                    del self.flags['vote NO']
                    # Wait for next transaction.
                    self.c_t_vote_req.restart()
                else:
                    print 'voting yes'
                    self.message = 'vote-yes'
                    # Wait for a precommit
                    self.c_t_prec.restart()
                self.log()
                self.send([m['id']], self.message_str())
                if 'crashAfterVote' in self.flags and self.id != self.coordinator:
                    del self.flags['crashAfterVote']
                    os._exit(1)
            # Only accept no votes if you're the coordinator.
            if m['message'] == 'vote-no' and self.id == self.coordinator:
                self.p_t_vote.suspend()
                self.message = 'abort'
                self.transaction['state'] = 'aborted'
                self.votes[m['id']] = False
                self.log()
                # Tell everyone we've aborted.
                self.send(self.alive, self.message_str())
                # Tell master you've aborted.
                self.send([-1], 'ack abort')
            # Only accept yes votes if you're the coordinator
            if m['message'] == 'vote-yes' and self.id == self.coordinator:
                self.votes[m['id']] = True
                # Everybody has voted yes!
                if len(self.alive) == len(self.votes) and all(self.votes.values()):
                    self.p_t_vote.suspend()
                    self.message = 'precommit'
                    self.transaction['state'] = 'precommitted'
                    if 'crashPartialPreCommit' in self.flags:
                        self.send(self.flags['crashPartialPreCommit'], self.message_str())
                        del self.flags['crashPartialPreCommit']
                        self.log()
                        os._exit(1)
                    else:
                        self.send(self.alive, self.message_str())
                        self.log()
                    # Start waiting for acks.
                    self.p_t_acks.restart()
            # Election protocol messages
            if m['message'] == 'lets-elect-coordinator':
                if self.stupid:
                    self.message = 'i-am-stupid-for-election'
                else:
                    self.message = 'take-my-alive-list-for-election'
                self.send([m['id']], self.message_str())
            # Here we wait
            if m['message'] == 'take-my-alive-list-for-election' or m['message'] == 'i-am-stupid-for-election':
                self.num_messages_received_for_election += 1
                l1 = self.alive
                l2 = m['alive']
                self.alive = [val for val in l1 if val in l2]
                if len(self.election_alive_list) == self.num_messages_received_for_election:
                    self.coordinator = min(self.alive)
                    print 'pid = %d, coord = %d' % (self.id, self.coordinator)
                    if self.coordinator == self.id:
                        # tell master about new coordinator
                        print 'decided new coordinator: ' + str(self.id)
                        self.send([-1], 'coordinator ' + self.id)

        print "end receive"
