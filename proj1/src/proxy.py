import zmq
import threading
import time
import sys
import signal
import logging

from datetime import datetime

from sqlalchemy import Column, Integer, LargeBinary, create_engine, func, delete
from sqlalchemy.orm import declarative_base, sessionmaker, scoped_session
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
from zeromq_entities import GenericZeroMqEntity
from utils import CobsHandler, bytes_to_int
from message import Message, RouterMessage, ReliableMessageFrame, GetMessage
from argument_parser import parse_arguments

logging.basicConfig(
    handlers=[logging.FileHandler("proxy.log"), logging.StreamHandler()],
    level=logging.INFO,
    format="%(levelname)s - %(asctime)s - [%(threadName)s]:%(message)s",
)


@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kw):
    return compiler.visit_insert(insert.prefix_with("OR REPLACE"), **kw)


Base = declarative_base()


class MessageCache(Base):
    __tablename__ = "messagecache"
    topic = Column("topic", LargeBinary, nullable=False, primary_key=True)
    num = Column("num", Integer, nullable=False, primary_key=True)
    content = Column("content", LargeBinary, nullable=False)


class SubscriberMessageRead(Base):
    __tablename__ = "subscriberread"
    topic = Column("topic", LargeBinary, nullable=False, primary_key=True)
    identifier = Column("identifier", LargeBinary, nullable=False, primary_key=True)
    num = Column("num", Integer, nullable=False)


class UnloadedTopic:
    def __init__(self, identifier, topic_loader):
        self.identifier = identifier
        self.topic_loader = topic_loader
        self.topic = None

    def __getattr__(self, name):
        if self.topic is None:
            logging.info(f"Loading topic {self.identifier} because of {name}")
            self.topic = self.topic_loader(self.identifier)
        return getattr(self.topic, name)


class Topic:
    def __init__(self, identifier, db_query, queue=dict(), subscribers=dict()):
        now = datetime.now().timestamp()
        self.identifier = identifier
        self.queue = queue
        self.subscribers = subscribers

        self.db_query = db_query
        self.unsubscribed = set()

        self.last_msgs_cached = now
        self.last_subscribers_cached = now
        self.last_unsubscribers_updated = now
        self.last_accessed = now

    def update_last_accessed(self):
        self.last_accessed = datetime.now().timestamp()

    def add_subscriber(self, subscriber_id):
        self.update_last_accessed()
        self.unsubscribed.discard(subscriber_id)
        if subscriber_id in self.subscribers:
            return False
        self.subscribers[subscriber_id] = self.current_message
        return True

    def remove_subscriber(self, subscriber_id):
        self.update_last_accessed()
        self.unsubscribed.add(subscriber_id)
        if subscriber_id in self.subscribers:
            del self.subscribers[subscriber_id]
            return True
        return False

    def subscriber_new_messages_count(self, subscriber_id):
        self.update_last_accessed()
        if subscriber_id not in self.subscribers:
            return -1
        return self.current_message - self.subscribers[subscriber_id]

    def add_message(self, message):
        self.update_last_accessed()
        if self.subscriber_count == 0:
            return False
        self.queue[self.current_message + 1] = message
        return True

    def get_messages(self, subscriber_id, suggested_num_msgs):

        self.update_last_accessed()
        wanted_msgs = min(
            max(0, suggested_num_msgs),
            self.subscriber_new_messages_count(subscriber_id),
        )
        msgs = []
        if wanted_msgs == 0:
            return msgs

        start_msg = self.subscribers[subscriber_id]
        min_msg = self.min_msg_in_queue

        if min_msg > (wanted_msgs + start_msg):
            #logging.error(f'primeira cond{subscriber_id} - {self.subscribers[subscriber_id]}')
            r = self.db_query(self.identifier, start_msg + 1, min_msg, wanted_msgs)
            msgs.extend(r)
        else:
            #logging.error(f'segunda cond {min_msg} {start_msg} {wanted_msgs} {subscriber_id} - {self.subscribers[subscriber_id]}')

            # add first what's in memory because querying from db
            # might lead to caching and trimming
            # thus invalidating old state
            if start_msg >= min_msg:
                for i in range(start_msg + 1, wanted_msgs + start_msg + 1):
                    msgs.append(self.queue[i])
            else:
                for i in range(min_msg, wanted_msgs + start_msg + 1):
                    msgs.append(self.queue[i])

                missing = wanted_msgs - len(msgs)
                if missing:
                    r = self.db_query(self.identifier, start_msg + 1, min_msg-1, missing)
                    r.extend(msgs)  # preserve order
                    msgs = r

        #logging.error(f'Sent {len(msgs)}(wanted:{wanted_msgs}) to {subscriber_id} - {self.subscribers[subscriber_id]}')
        self.subscribers[subscriber_id] += len(msgs)
        return msgs

    def __str__(self):
        return f"Topic:{self.identifier} last accessed:{self.last_accessed}"

    def __repr__(self):
        return str(self)

    @property
    def current_message(self):
        if len(self.queue) == 0:
            return 0
        return max(self.queue.keys())

    @property
    def min_msg_in_queue(self):
        return min(self.queue.keys())

    @property
    def subscriber_count(self):
        return len(self.subscribers)


class Proxy(GenericZeroMqEntity):

    subscribe_decoder = CobsHandler("id", "topic")
    get_decoder = CobsHandler("id", "topic", "num")
    put_decoder = CobsHandler("topic", "msg")

    def __init__(
        self,
        max_get_msgs,
        max_topics_in_memory,
        topic_queue_size,
        max_topics_per_cache_call,
        topic_unload_interval,
        cache_interval,
        unsub_interval,
        stale_message_interval,
    ):
        super().__init__()
        self.stop = False
        self._sub_socket = self._context.socket(zmq.ROUTER)
        self._pub_socket = self._context.socket(zmq.ROUTER)
        self.topics = dict()

        self.max_get_msgs = max_get_msgs
        self.max_topics_per_cache_call = max_topics_per_cache_call
        self.max_topic_queue = topic_queue_size
        self.max_topics_in_memory = max_topics_in_memory

        self.topic_unload_interval = topic_unload_interval
        self.cache_interval = cache_interval
        self.unsub_interval = unsub_interval
        self.stale_message_interval = stale_message_interval

        engine = create_engine(
            "sqlite:///cache.db",
        )
        Base.metadata.create_all(bind=engine)
        self._session_factory = sessionmaker(bind=engine)
        self.session_creator = scoped_session(self._session_factory)

        self.load_from_disk()
        signal.signal(signal.SIGINT, self.signal_handler)

    def load_topic_from_disk(self, topic):

        session = self.session_creator()
        msgs = (
            session.query(MessageCache)
            .filter(MessageCache.topic == topic)
            .order_by(MessageCache.num.desc())
            .limit(self.max_topic_queue)
            .all()
        )

        subscribers = (
            session.query(SubscriberMessageRead)
            .filter(SubscriberMessageRead.topic == topic)
            .order_by(SubscriberMessageRead.num.desc())
            .all()
        )

        self.session_creator.remove()

        msg_dict = dict()
        for msg in msgs:
            msg_dict[msg.num] = msg.content

        subscribers_dict = dict()
        for sub in subscribers:
            subscribers_dict[sub.identifier] = sub.num

        t = Topic(
            topic, self._retrieve_msgs, queue=msg_dict, subscribers=subscribers_dict
        )
        self.topics[topic] = t
        return t

    def load_from_disk(self):

        for row in self.session_creator.query(MessageCache.topic).distinct():
            identifier = row[0]
            self.topics[identifier] = UnloadedTopic(
                identifier, self.load_topic_from_disk
            )

    def _retrieve_msgs(self, topic_id, low, high, num_msgs):

        session = self.session_creator()
        res = (
            session.query(MessageCache)
            .filter(MessageCache.topic == topic_id)
            .filter(MessageCache.num.between(low, high))
            .order_by(MessageCache.num.asc())
            .limit(num_msgs)
            .all()
        )
        self.session_creator.remove()
        return [x.content for x in res]

    def start(self, host, port_sub, port_pub):
        self.host = host
        self.port_sub = port_sub
        self.port_pub = port_pub

        self._sub_socket.bind(f"tcp://{host}:{port_sub}")
        self._pub_socket.bind(f"tcp://{host}:{port_pub}")

    def get_topic_handle(self, topic):

        if topic in self.topics:
            return self.topics[topic]

        t = Topic(topic, self._retrieve_msgs)
        self.topics[topic] = t
        return t

    def handle_subscribe(self, content):
        components = Proxy.subscribe_decoder.get_components(content)
        result = self.get_topic_handle(components["topic"]).add_subscriber(
            components["id"]
        )

        if not result:
            return ReliableMessageFrame.encode(Message.ALREADY_SUBSCRIBED)
        return ReliableMessageFrame.encode(Message.ACK)

    def handle_unsubscribe(self, content):
        components = Proxy.subscribe_decoder.get_components(content)
        result = self.get_topic_handle(components["topic"]).remove_subscriber(
            components["id"]
        )

        if not result:
            return ReliableMessageFrame.encode(Message.NOT_SUBSCRIBED)

        return ReliableMessageFrame.encode(Message.ACK)

    def handle_get(self, content):
        components = Proxy.get_decoder.get_components(content)

        topic_handle = self.get_topic_handle(components["topic"])
        new_messages = topic_handle.subscriber_new_messages_count(components["id"])
        components["num"] = bytes_to_int(components["num"])

        if new_messages < 0:
            return ReliableMessageFrame.encode(Message.NOT_SUBSCRIBED)
        elif new_messages == 0:
            return ReliableMessageFrame.encode(Message.NO_MESSAGES)

        msgs = topic_handle.get_messages(components["id"], components["num"])
        ret = ReliableMessageFrame.encode(Message.ACK, GetMessage.encode(msgs))
        return ret

    def handle_put(self, content):
        components = Proxy.put_decoder.get_components(content)
        result = self.get_topic_handle(components["topic"]).add_message(
            components["msg"]
        )

        if not result:
            return ReliableMessageFrame.encode(Message.NO_SUBSCRIBER)
        return ReliableMessageFrame.encode(Message.ACK)

    msg_dispatcher = {
        Message.SUBSCRIBE: handle_subscribe,
        Message.UNSUBSCRIBE: handle_unsubscribe,
        Message.GET: handle_get,
    }

    def handle_msg(self, msg_frame):

        components = ReliableMessageFrame.decode(msg_frame)

        if components["type"] not in Proxy.msg_dispatcher:
            raise ValueError("Unhandled type")

        return Proxy.msg_dispatcher[components["type"]](self, components["content"])

    def activate_pub(self):
        while not self.stop:
            content = self._pub_socket.recv_multipart()

            if self.stop:
                break
            try:
                generic_msg = RouterMessage.from_multipart(content)
                components = ReliableMessageFrame.decode(generic_msg)
                if components["type"] != Message.PUT:
                    continue

                response = self.handle_put(components["content"])
                self._pub_socket.send_multipart(generic_msg.response(response))
            except Exception as e:
                logging.error(f"Got exception {e}")

    def activate_sub(self):
        while not self.stop:
            content = self._sub_socket.recv_multipart()

            if self.stop:
                break

            response = None
            try:
                generic_msg = RouterMessage.from_multipart(content)
                response = self.handle_msg(generic_msg)
            except Exception as e:
                logging.error(f"Got exception {e}")
                response = ReliableMessageFrame.encode(Message.INVALID)
            self._sub_socket.send_multipart(generic_msg.response(response))

    def _cache_topic_msgs(self, session, topic, now):
        # cache everything

        max_num_cached = (
            session.query(func.max(MessageCache.num))
            .filter(MessageCache.topic == topic.identifier)
            .scalar()
        )

        tq = [(x,y) for x,y in topic.queue.items()]
        for num, content in tq:
            if max_num_cached is not None and num <= max_num_cached:
                continue
            logging.info(f"Caching message {topic.identifier}:{num}")
            session.add(MessageCache(topic=topic.identifier, num=num, content=content))

        session.commit()

        # trim

        if len(topic.queue) > self.max_topic_queue:
            remove_keys = sorted(topic.queue.keys())[
                : len(topic.queue) - self.max_topic_queue
            ]
            for key in remove_keys:
                logging.info(f"Removing key {key} from {topic.identifier}")
                del topic.queue[key]

        # update last_cached
        topic.last_msgs_cached = now

    def _cache_topic_subscribers(self, session, topic, now):
        logging.info(f"Caching topic subscribers {topic.identifier}")

        # cache everything

        si = [(x,y) for x,y in topic.subscribers.items()]
        for subscriber_id, cur_msg in si:
            session.add(
                SubscriberMessageRead(
                    topic=topic.identifier, num=cur_msg, identifier=subscriber_id
                )
            )

        session.commit()

        # update last_cached
        topic.last_subscribers_cached = now

    def _remove_sub(self, session, topic, now):

        unsubs = topic.unsubscribed.copy()
        topic.unsubscribed.clear()

        for entry in unsubs:
            logging.info(f"Removing subscriber {entry} of {topic.identifier}")
            session.execute(
                delete(SubscriberMessageRead).where(
                    SubscriberMessageRead.identifier == entry
                )
            )

        session.commit()
        topic.last_unsubscribers_updated = now

    def activate_caching_subscribers(self):

        while not self.stop:
            now = datetime.now().timestamp()

            topics_to_cache = sorted(
                self.loaded_topics, key=lambda x: x.last_subscribers_cached
            )[: self.max_topics_per_cache_call]

            if topics_to_cache:
                session = self.session_creator()
                for topic in topics_to_cache:
                    self._cache_topic_subscribers(session, topic, now)
                self.session_creator.remove()
            time.sleep(self.cache_interval)

    def activate_caching_msgs(self):

        while not self.stop:
            now = datetime.now().timestamp()

            topics_to_cache = sorted(
                self.loaded_topics, key=lambda x: x.last_msgs_cached
            )[: self.max_topics_per_cache_call]

            if topics_to_cache:

                session = self.session_creator()
                for topic in topics_to_cache:
                    self._cache_topic_msgs(session, topic, now)
                self.session_creator.remove()
            time.sleep(self.cache_interval)

    def activate_unsubscribe(self):
        while not self.stop:
            now = datetime.now().timestamp()

            topics_to_unsub = sorted(
                self.loaded_topics, key=lambda x: x.last_unsubscribers_updated
            )[: self.max_topics_per_cache_call]

            if topics_to_unsub:
                session = self.session_creator()
                for topic in topics_to_unsub:
                    self._remove_sub(session, topic, now)
                self.session_creator.remove()
            time.sleep(self.unsub_interval)

    def _unload_topic(self, session, topic, now):
        logging.info(f"Unloading {topic.identifier}")
        self._remove_sub(session, topic, now)
        self._cache_topic_msgs(session, topic, now)
        self._cache_topic_subscribers(session, topic, now)

    def activate_topic_unloader(self):
        while not self.stop:

            now = datetime.now().timestamp()
            topics_to_unload = sorted(
                self.loaded_topics, key=lambda x: x.last_accessed, reverse=True
            )[self.max_topics_in_memory :]

            for topic in topics_to_unload:
                self.topics[topic.identifier] = UnloadedTopic(
                    topic.identifier, self.load_topic_from_disk
                )

            if topics_to_unload:

                session = self.session_creator()

                for topic in topics_to_unload:
                    self._unload_topic(session, topic, now)
                self.session_creator.remove()

            time.sleep(self.topic_unload_interval)

    def activate_stale_msg_removal(self):
        while not self.stop:

            now = datetime.now().timestamp()
            topics = list(self.topics.values())

            session = self.session_creator
            for topic in topics:
                min_num = (
                    session.query(func.min(SubscriberMessageRead.num))
                    .filter(SubscriberMessageRead.topic == topic.identifier)
                    .scalar()
                )
                if min_num is None:
                    continue
                logging.info(
                    f"Removing stale messages for {topic.identifier}, less than {min_num}"
                )
                session.execute(delete(MessageCache).where(MessageCache.num < min_num))
                session.commit()

            self.session_creator.remove()
            time.sleep(self.stale_message_interval)

    def activate(self):
        pub_t = threading.Thread(target=self.activate_pub, name="Publisher Handler")
        sub_t = threading.Thread(target=self.activate_sub, name="Subscriber Handler")
        caching_msgs_t = threading.Thread(
            target=self.activate_caching_msgs, name="Caching Msgs"
        )
        caching_subscribers_t = threading.Thread(
            target=self.activate_caching_subscribers, name="Caching Subscribers"
        )
        unsubscribing_t = threading.Thread(
            target=self.activate_unsubscribe, name="Remove Unsubscribers"
        )
        unloader_t = threading.Thread(
            target=self.activate_topic_unloader, name="Topic Unloader"
        )
        stale_msg_removal_t = threading.Thread(
            target=self.activate_stale_msg_removal, name="Stale Message Removal"
        )

        self.threads = [
            pub_t,
            sub_t,
            caching_msgs_t,
            caching_subscribers_t,
            unsubscribing_t,
            unloader_t,
            stale_msg_removal_t,
        ]

        for t in self.threads:
            t.start()

        while len(self.threads):
            for t in self.threads:
                t.join()
                if not t.is_alive():
                    logging.warning(f"{t.name} quit")
            self.threads = list(filter(lambda x: x.is_alive(), self.threads))

    def signal_handler(self, handler, frame):

        if self.stop:
            logging.error("Quit abruptly")
            sys.exit(1)

        self.stop = True

        now = datetime.now().timestamp()
        topics = list(self.loaded_topics)
        session = self.session_creator

        logging.warning("Saving unsubscribers")
        for t in topics:
            self._remove_sub(session, t, now)

        logging.warning("Saving subscribers")
        for t in topics:
            self._cache_topic_subscribers(session, t, now)

        logging.warning("Saving messages")
        for t in topics:
            self._cache_topic_msgs(session, t, now)
        logging.warning(
            "Everything was saved, trying to make the threads quit gracefully"
        )

        exit_socket = self._context.socket(zmq.DEALER)
        exit_socket.connect(f"tcp://{self.host}:{self.port_pub}")
        exit_socket.send(b"")
        exit_socket.close()

        exit_socket = self._context.socket(zmq.DEALER)
        exit_socket.connect(f"tcp://{self.host}:{self.port_sub}")
        exit_socket.send(b"")
        exit_socket.close()

    @property
    def loaded_topics(self):
        return filter(lambda x: isinstance(x, Topic), self.topics.values())


def main():

    args = parse_arguments(
        {"port", "connect_type"},
        [
            ("--port_sub", str, True, None, None),
            ("--port_pub", str, True, None, None),
            ("--max_get_msgs", int, False, None, 5),
            ("--max_topics_in_memory", int, False, None, 2),
            ("--topic_queue_size", int, False, None, 5),
            ("--max_topics_per_cache_call", int, False, None, 1),
            ("--topic_unload_interval", int, False, None, 10),
            ("--cache_interval", int, False, None, 10),
            ("--unsub_interval", int, False, None, 30),
            ("--stale_msg_interval", int, False, None, 30),
        ],
    )

    getter = Proxy(
        args.max_get_msgs,
        args.max_topics_in_memory,
        args.topic_queue_size,
        args.max_topics_per_cache_call,
        args.topic_unload_interval,
        args.cache_interval,
        args.unsub_interval,
        args.stale_msg_interval,
    )
    logging.info("Proxy created")

    getter.start(args.host, args.port_sub, args.port_pub)
    getter.activate()


if __name__ == "__main__":
    main()
