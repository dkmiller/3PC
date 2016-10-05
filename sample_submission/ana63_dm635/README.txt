We implemented three phase commit using Python 2. There is a collection of 
processes, with 'internal ids' ranging from 0 to N-1, where N is the total 
number of 'backend' processes. One of the backend processes is designated 
as coordinator; it receives commands from a master (provided by the graders) 
and directs a run of 3PC. Each backend process listens on port 20000+i, where 
i is the process's 'internal id'. The process has one thread listening on 
20000+i, one thread communicating with the master, and N threads maintaining 
sockets to each of the open ports. In addition, there are five timer threads, 
responsible for detecting various timeouts.

The file process.py contains the code which handles the actual connections 
between backend processes. Each instance of process.py has a Client object, 
which governs the logic of three-phase commit. The two (process and Client) 
communicate via a minimalist API. Namely, each Client object is given a 
send(pids[], msg) function which sends msg to each of the given processes, 
then returns the array of processes which successfully received msg. Moreover, 
each Client has functions receive(msg) and receive_master(msg), which are 
called when process.py gets a message from another backend / the master. 

We are able to minimize logging by 'piggybacking' almost the entire state of 
a Client on each backend message. This can be done because, for our purposes, 
there are at most 10 backend processes and the internal hash table will be 
small. Thus, the serialized state of a Client will take up at most 500 bytes, 
which easily fits inside a single TCP block. Thus there is no extra overhead 
between sending a short (say 25 chars) message and the slightly longer state 
that we include. 

One possible quibble: our code does not handle the case where participants of 
3PC successfully receive a VOTE-REQ, but crash before voting. The structure of 
the possible 'special flags' that a process can receive is such that this case 
can never occur, assuming no crashes other than those given by these special 
cases. 

To compile the project, run the script build in this folder in a *nix 
environment. 
