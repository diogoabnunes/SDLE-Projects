# SDLE Project

This project was design and implemented for the Large Scale Distributed Systems class and consists of a reliable publish-subscribe service with exactly-once delivery

SDLE Project for group T2G13.

Group members:

1. Rita Mota (up201703964@up.pt)
2. José Silva (up201705591@up.pt)
3. Mariana Soares (up201605775@up.pt)
4. Diogo Nunes (up201808546@up.pt)


## Pre-requisites

* Python 3.6+
* Python virtual environment - not necessary but highly recommened 

```bash
python3 -m venv env #creates a virtual environment called 'env'
source env/bin/activate #activates the virtual environment, the terminal should now be prefixed with (env)
env\Scripts\activate #equivalent command for Windows users
```

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the necessary dependencies.
If you're in the source folder:

```bash
pip install -r requirements.txt
```

## Usage


### Tester

The tester is used to perform validation on the broker. The broker must be turned on seperatelly.

```bash
python tester.py NUM_MSGS NUM_SUBS ['equal']
python tester.py 500 300 
```
- **num_msgs**: number of messages to be put in a topic
- **num_subs**: number of subscribers of a topic
- **msg_uniq**: if messages should be unique 


### Broker (proxy)

```bash
python proxy.py --host HOST --port_sub PORT_SUB --port_pub PORT_PUB 
                [--max_get_msgs MAX_GET_MSGS] 
                [--max_topics_in_memory MAX_TOPICS_IN_MEMORY] 
                [--topic_queue_size TOPIC_QUEUE_SIZE]
                [--max_topics_per_cache_call MAX_TOPICS_PER_CACHE_CALL]
                [--topic_unload_interval TOPIC_UNLOAD_INTERVAL]
                [--cache_interval CACHE_INTERVAL]
                [--unsub_interval UNSUB_INTERVAL]
                [--stale_msg_interval STALE_MSG_INTERVAL] 
python proxy.py --host 127.0.0.1 --port_sub 7070 --port_pub 6060
```
- **host**: IP address of the broker
- **port_sub**: endpoint where the subscribers are running
- **port_pub**: endpoint where the publishers are running
- **max_get_msgs**: maximum number of messages the subscribers can receive in one get() call (default 5)
- **max_topics_in_memory**: maximum number of topics that can be stored in memory (default 2)
- **topic_queue_size**: maximum number of messages that can be in the queue of a specific topic (default 5)
- **max_topics_per_cache_call**: maximum number of topics to be cached per execution (default 1)
- **topic_unload_interval**: topics will be loaded from memory into storage every x seconds (default 10)
- **cache_interval**: the topics in cache are updated every x seconds (default 10)
- **unsub_interval**: list of unsubscribers kept in cache is updated every x seconds (default 30)
- **stale_msg_interval**: every x seconds the broker checks which is the most delayed subscriber and deletes messages older than the oldest message still to be delivered from the database (default 30)


### Publisher

```bash
python publisher.py --host HOST --port PORT --connect_type {connect,bind}
python publisher.py --host 127.0.0.1 --port 6060 --connect_type connect
```
- **host**: IP address of the publishers
- **port**: endpoint where the publishers are running
- **connect_type**: how the node should establish connection with the broker (default connect)

### Subscriber

```bash
python subscriber.py --host HOST --port PORT --connect_type {connect,bind} --id ID
python subscriber.py --host 127.0.0.1 --port 7070 --connect_type connect --id 2
```
- **host**: IP address of the subscribers
- **port**: endpoint where the subscribers are running
- **connect_type**: how the node should establish connection with the broker (default connect)
