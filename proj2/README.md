# SDLE Project

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

```
python node.py --name NAME
               [-h]         
               [--host_dht HOST_DHT] 
               [--port_dht PORT_DHT]
               [--host_comm HOST_COMM] 
               [--port_comm PORT_COMM] 
               [--host_boots HOST_BOOTS] 
               [--port_boots PORT_BOOTS]
               [--max_saved_msgs MAX_SAVED_MSGS] 
               [--request_timeout REQUEST_TIMEOUT]   
               [--replicate_min_interval REPLICATE_MIN_INTERVAL]
               [--replicate_max_interval REPLICATE_MAX_INTERVAL]
               [--trim_and_save_interval TRIM_AND_SAVE_INTERVAL]
               [--delete_invalid_entries_interval DELETE_INVALID_ENTRIES_INTERVAL]     
               [--dht_share_stats_interval DHT_SHARE_STATS_INTERVAL]
               [--dht_announce_interval DHT_ANNOUNCE_INTERVAL]
               [--missing_fixer_interval MISSING_FIXER_INTERVAL]
               [--get_msgs_interval GET_MSGS_INTERVAL]
               [--persist_following_interval PERSIST_FOLLOWING_INTERVAL]
               [--remove_entry_seconds REMOVE_ENTRY_SECONDS]
python node.py --name node1
```

- **name**: name attributed to the node
- **host_dht**: IP address of the distributed hash table
- **port_dht**: endpoint where the dht is running
- **host_comm**: IP address of the http communication
- **port_comm**: endpoint where the http communication is running
- **host_boots**: IP address of the dht node to which the service connects
- **port_boots**: endpoint where the dht node to which the service connects is running
- **max_saved_msgs**: maximum number of messages from that node to be kept in memory
- **request_timeout**: number of seconds before an http request timeout
- **replicate_min_interval**: minimum number of seconds for a node to replicate a message
- **replicate_max_interval**: maximum number of seconds for a node to replicate a message
- **trim_and_save_interval**: time interval in which the messages kept in memory are trimmed and persisted to the database 
- **delete_invalid_entries_interval**: time interval in which messages with an invalid signature are deleted
- **dht_share_stats_interval**: time interval in which the node updates the dht on what information he has on other nodes
- **dht_announce_interval**: time interval in which the node must acknowledge its presence
- **missing_fixer_interval**: time interval in which missing messages are detected and found 
- **get_msgs_interval**: time interval in which a message request is done
- **persist_following_interval**: time interval in which the nodes being followed are persisted to the database 
- **remove_entry_seconds**: how many seconds after being posted a message can still be accepted
