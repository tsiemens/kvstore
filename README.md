# KVStore: A Distributed Key Value Store
EECE 411 Project Group 4

This distributed service is separated into two components: The node monitoring service, and the distributed key value store.

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
