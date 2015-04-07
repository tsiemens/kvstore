# KVStore: A Distributed Key Value Store
EECE 411 Project Group 5

This distributed service is separated into two components: The node monitoring service, and the distributed key value store.

## Key Value Store

#### Architecture & Design
For the replication component, our system uses a quorum based algorithm. When a node (intermediate node) receives a request
from a client, it first figures out who the responsible node (primary node) is for that value as well as all the back up nodes. 
For a PUT or REMOVE command, the intermediate node sends a request to the primary and all the backups requesting their timestamps.
Following that, a PUT or REMOVE command is sent to the primary and backup nodes containing the highest timestamp + 1. For GET 
commands, the primary as well as all the backups are queried and the value with the highest timestamp is returned. To make this
algorithm a quorum algorithm, the amount of responses required to correctly execute a command is K/2 + 1, where K is the number
of backups. 

#### Additional Response Codes
* 0x09: The message structure for the command was invalid (eg. mismatched value length, missing data)

### Replication Test Cases
The following test cases were performed in this order:

#### Test 1
* Get a value with a key that doesn't exist from Node 1 (Result: ERROR)
* Get a value with a key that doesn't exist from Node 2 (Result: ERROR)
* Get a value with a key that doesn't exist from Node 3 (Result: ERROR)
* Put a value with key 'hello' to Node 1 (Result: Success)
* Get a value with key 'hello' from Node 1 (Result: Success)
* Get a value with key 'hello' from Node 2 (Result: Success)
* Get a value with key 'hello' from Node 3 (Result: Success)
* Kill node 1
* Get a value with key 'hello' from Node 2 (Result: Success)
* Get a value with key 'hello' from Node 3 (Result: Success)

#### Test 2
* Get a value with a key that doesn't exist from Node 1 (Result: ERROR)
* Get a value with a key that doesn't exist from Node 2 (Result: ERROR)
* Get a value with a key that doesn't exist from Node 3 (Result: ERROR)
* Put a value with key 'hello' to Node 1 (Result: Success)
* Get a value with key 'hello' from Node 1 (Result: Success)
* Get a value with key 'hello' from Node 2 (Result: Success)
* Get a value with key 'hello' from Node 3 (Result: Success)
* Kill node 2
* Get a value with key 'hello' from Node 1 (Result: Success)
* Get a value with key 'hello' from Node 3 (Result: Success)

#### Test 3
* Get a value with a key that doesn't exist from Node 1 (Result: ERROR)
* Get a value with a key that doesn't exist from Node 2 (Result: ERROR)
* Get a value with a key that doesn't exist from Node 3 (Result: ERROR)
* Put a value with key 'hello' to Node 1 (Result: Success)
* Get a value with key 'hello' from Node 1 (Result: Success)
* Get a value with key 'hello' from Node 2 (Result: Success)
* Get a value with key 'hello' from Node 3 (Result: Success)
* Kill node 3
* Get a value with key 'hello' from Node 2 (Result: Success)
* Get a value with key 'hello' from Node 1 (Result: Success)

#### Test 4
* Get a value with a key that doesn't exist from Node 1 (Result: ERROR)
* Get a value with a key that doesn't exist from Node 2 (Result: ERROR)
* Get a value with a key that doesn't exist from Node 3 (Result: ERROR)
* Put a value with key 'hello' to Node 1 (Result: Success)
* Get a value with key 'hello' from Node 1 (Result: Success)
* Get a value with key 'hello' from Node 2 (Result: Success)
* Get a value with key 'hello' from Node 3 (Result: Success)
* Kill node 1
* Remove value with key 'hello' from Node 2 (Result: Success)
* Get a value with key 'hello' from Node 3 (Result: ERROR)

#### Performance
We ran the following performance metrics:


Testing performance of 100 synchronous puts...
real	0m38.460s
user	0m0.479s
sys	    0m0.648s

Testing performance of 100 synchronous gets...
real	0m23.732s
user	0m0.538s
sys	    0m0.617s
Errors: 3

Testing performance of 100 synchronous deletes...
real	0m38.096s
user	0m0.510s
sys	    0m0.584s
Errors: 4

## Monitoring Service
In order to reuse some of the more general protocols, the monitoring service is built into the general functionality of the key value store node executable, as is the main server.

#### Architecture
At the center of the monitoring service is a well known host, which hosts an http server. At regular intervals, this server choses a random node, out of a list of other known node participants, and sends it a message. This message initiates a gossiping epidemic algorithm, that propogates throughout the rest of the nodes in the system. After receiving the first message in the set, a node will send a list of stats about itself back to the central server to be compiled and displayed.

Additionally, if the central server does not receive any messages from a given node for some time, it will declare it 'not responding' in its report.

#### Limitations
Currently, the status server expects a very specific set of stats from each node, in order to format it correctly. As such, in order to modify the contents shown on the status server, compiled code would need to be modified, and uploaded to all participating nodes.

Bootstrapping is also somewhat limited. A list (separate file) of all potentially participating nodes is required to be present on each node. Should there be new nodes added to the list, this file would need to be updated.

This system would arguably be less scalable in circumstances where many many nodes were all attempting to communicate with the central server in rapid succession.

#### Additional features
In addition to the standard monitor data message accepted by each node, arbitrary bash scripts can also be executed. For scalability, these are also propogated epidemically. This is useful, for example, if a new node was added, the process could use sed to modify the config file, then fork a process to restart the node. Unfortunately this does not yet support uploading binary files.
