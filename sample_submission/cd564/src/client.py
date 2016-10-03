import json

class Client:
    def message(self, p, type, **kwargs):
        msg = {
            'sender' : self.id,
            # Current transaction number.
            'current_transaction' : self.transaction,
            # Process the sender believes is the coordinator.
            'coordinator' : self.coordinator,
            'message_type' : type,
            # Processes the sender believes are alive.
            'alive_list' : self.alive,
            'recipient' : p
        }
        for key in enumerate(kwargs):
            msg[key] = kwargs[key]
        return json.puts(msg)
    # Broadcasts message of given type and internally updates alive list. The
    # broadcast goes to all messages, including the sender.
    def broadcast(self, type, **kwargs):
        for p in xrange(self.N):
            send_string = self.message(p, type, **kwargs)
            # Process p received broadcast.
            if self.send(p, send_string):
                self.alive[p] = True
            # Process p didn't receive broadcast.
            elif p in self.alive:
                del self.alive[p]

    def __init__(self, pid, num_procs, send):
        # Total number of processes (not including master).
        self.N = num_procs
        # Process id.
        self.id = pid
        # Internal hash table of URL : song_name.
        self.data = {}
        # Process is coordinator.
        self.coordinator = pid
        # List of processes that we think are alive.
        self.alive = {pid : True}
        # Current state.
        self.state = 'wait-commit'
        # Action that the current transaction will perform (currently nothing).
        self.action = lambda *args, **kwargs: None
        # Send functionL
        self.send = send

        with open('%dlog.json' % self.id, 'w+') as log:
            try:
                # Internal hash map URL : "Song name".
                self.log = json.load(log)
                # Current transaction counter
                self.state.transaction = len(self.log)-1
                # Last transaction not completed.
                if True: # TODO: good logging.
                    self.state = 'recover'
                    self.broadcast('help-me')
            except ValueError: # First start, so empty log.
                self.log = []
                self.transaction = 0
        # Find out who is alive and who is the coordinator.
        self.broadcast('just-woke')
        if len(self.alive) == 1: # Only live process
            self.coordinator = self.id
            # Tell master this process is now coordinator.
            self.send(-1, 'coordinator %d' % self.id)

    def receive(self, s):
        msg = json.loads(s)
        if msg['message_type'] == 'am_alive':
            self.coordinator = msg['coordinator']
            self.transaction = msg['transaction']

        # This process is the coordinator.
        if self.coordinator == self.id:
            # Receive a yes vote.
            if msg['message_type'] == 'vote-yes':
                self.votes[msg['sender']] = True
                # All votes (except possibly coordinator) yes!
                if any(self.votes.values()):
                    self.broadcast('pre-commit')
            # Receive a no vote.
            elif msg['message_type'] == 'vote-no':
                self.votes[msg['sender']] = False
                self.state = 'abort'
                self.broadcast('abort')
        # All processes (including coordinator) enter this code block.
        if msg['message_type'] == 'vote-req':
            self.transaction += 1
            def action():
                if msg['action'] == 'add':
                    self.data[message['song_name']] = message['URL']
                elif msg['action'] == 'delete':
                    del self.data[message['song_name']]
            self.action = action
            self.send(self.coordinator, self.message('vote-yes'))
            self.state = 'voted-yes'
        elif msg['message_type'] == 'precommit':
            self.send(self.coordinator, self.message('ack'))
        elif msg['message_type'] == 'commit':
            self.action()
            self.state = 'committed'


    def receive_master(self, s):
        parts = s.split()
        # Begin three-phase commit.
        if parts[1] == 'add':
            # Start a new transaction.
            self.transaction += 1
            self.votes = {}
            broadcast('vote-req', action='add', song_name=parts[2], URL=parts[3])
        elif parts[1] == 'delete':
            self.transaction += 1
            self.votes = {}
            broadcast('vote-req', action='delete', song_name=parts[2], URL=parts[3])
