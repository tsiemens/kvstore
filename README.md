# KVStore: A Distributed Key Value Store
EECE 411 Project Group 4

This distributed service is separated into two components: The node monitoring service, and the distributed key value store.

## Key Value Store

#### Architecture & Design
For the first iteration of the key value store, the system was kept simple. Each node can be started on any port from the command line, and will configure itself based on a small configuration file, though this does not do anything meaningful for A3 at this point. The node will wait for incoming messages on the port, and on receiving a datagram, will spawn a new thread to handle the message. The message handler parses the message, based on the command, and returns an appropriate response. Internally, the handling code surrounding this was designed to be as extensible as possible, so that further message commands could be added in the future for inter-node communication.

Additionally, the nodes can currently execute a membership protocol. They use an initial contact point (chose one out of well known host in config file), after which they can contact any node to update their membership. This has no impact on A3, however the implementation of A2 was modified to use this membership list in place of the static list of nodes previously used.

#### Additional Response Codes
* 0x09: The message structure for the command was invalid (eg. mismatched value length, missing data)

#### Test Cases
The following test cases were performed in this order:
* GET a value with a key that doesn't exist in the store (Result: ERROR)
* PUT a new value 'hello' with key X (Result: Success)
* GET a value with key X (Result: 'hello', Success)
* GET a value with a key that doesn't exist in the store (Result: ERROR)
* PUT a new value 'world' with key X (Result: Success)
* GET a value with key X (Result: 'world', Success)
* REMOVE a value with a key that doesn't exist in the store (Result: ERROR)
* REMOVE a value with key X (Result: Success)
* REMOVE a value with key X (Result: ERROR)

#### Performance
We ran the following performance metrics:

Testing performance of 100 asynchronous puts...
real	0m1.480s
user	0m0.287s
sys	0m0.349s

Testing performance of 100 synchronous puts...
real	0m1.708s
user	0m0.279s
sys	0m0.326s

Testing performance of 100 synchronous gets...
real	0m1.619s
user	0m0.262s
sys	0m0.366s

Testing performance of 100 synchronous deletes...
real	0m1.874s
user	0m0.272s
sys	0m0.430s


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
