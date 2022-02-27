rm proxy.log > /dev/null
rm cache.db > /dev/null
which python
python proxy.py --host 127.0.0.1 --port_sub  6969 --port_pub 7070 --max_get_msgs 50 --topic_queue_size 400

