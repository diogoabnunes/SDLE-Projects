import logging
from argument_parser import parse_arguments
import asyncio
from quart import Quart, render_template, redirect, url_for, request
import socket
from kademlia.network import Server
import ujson
from hashlib import sha256
from binascii import hexlify
from random import randint
import ntplib
from datetime import datetime, timezone
from collections import defaultdict
import aiohttp
from functools import partial, lru_cache
from enum import Enum

from Crypto.Hash import SHA256
from Crypto.Signature import PKCS1_v1_5
from Crypto.PublicKey import RSA


from sqlalchemy.future import select
from sqlalchemy import Column, Text, Integer, func, delete
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql.expression import Insert
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import UniqueConstraint


app = Quart(__name__)


handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

kad_log = logging.getLogger('kademlia')
kad_log.addHandler(handler)
kad_log.setLevel(logging.ERROR)

log = logging.getLogger('chat')
log.addHandler(handler)
log.setLevel(logging.INFO)

Base = declarative_base()

g_Agg = None

@compiles(Insert)
def _prefix_insert_with_ignore(insert, compiler, **kw):
        return compiler.visit_insert(insert.prefix_with("OR IGNORE"), **kw)



@lru_cache(maxsize=None)
def get_private_key(private_key_owner):
    with open(f'keys/{private_key_owner}.priv', 'r') as fp:
        return RSA.importKey(fp.read())


@lru_cache(maxsize=None)
def get_public_key(private_key_owner):
    with open(f'keys/{private_key_owner}.pub', 'r') as fp:
        return RSA.importKey(fp.read())

def sign_content(content, private_key_owner):
    private_key = get_private_key(private_key_owner)
    digest = SHA256.new()
    digest.update(content.encode())
    return PKCS1_v1_5.new(private_key).sign(digest).hex()

def verify_content(content, public_key_owner, signature):
    public_key = get_public_key(public_key_owner)
    digest = SHA256.new()
    digest.update(content.encode())
    return PKCS1_v1_5.new(public_key).verify(digest, signature)


class SignedDict:
    def __init__(self, **kwargs):
        self.d = dict()

        for k,v in kwargs.items():
            self.d[k] = v

    @property
    def content(self):
        c = ''
        keys = self.d.keys()
        kvs = map(lambda y: (y, self.d[y]), filter(lambda x: x != 'signature' and x != 'signature_owner', sorted(keys)))
        for k,v in kvs:
            c += f'{k}-{v}'
        return c

    @property
    def signed(self):
        self.d['signature_owner'] = g_Agg.name
        self.d['signature'] = sign_content(self.content, self.d['signature_owner'])
        return self.d

    @property
    def signed_json(self):
        return ujson.dumps(self.signed)


    @staticmethod
    def is_valid(extern_d):
        tmp = SignedDict(**extern_d)
        return extern_d['signature'] == sign_content(tmp.content, extern_d['signature_owner'])


class GetMsgType(Enum):
    KEEP_EPHEMERAL = 0
    FILTER_EPHEMERAL = 1
    REMOVE_EPHEMERAL = 2
    ONLY_EPHEMERAL = 3

def filter_ephemeral_msg(get_msg_type, current_timestamp, msg):

    if get_msg_type == GetMsgType.KEEP_EPHEMERAL:
        return True

    if get_msg_type == GetMsgType.REMOVE_EPHEMERAL:
        return msg.until is None

    if get_msg_type == GetMsgType.FILTER_EPHEMERAL:
        return msg.until is None or msg.until > current_timestamp

    if get_msg_type == GetMsgType.ONLY_EPHEMERAL:
        return msg.until is not None and msg.until > current_timestamp

    raise ValueError("Shouldn't be reached")

class Following(Base):
    __tablename__ = "following"
    name = Column("name", Text, nullable=False, primary_key=True)

    def __repr__(self):
        return f'{self.name}'

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name

class Message(Base):
    __tablename__ = "stored_messages"
    ignored_id = Column("ignored_id", Integer, primary_key=True)
    message_id = Column("message_id", Integer, autoincrement=True, nullable=False)
    publisher = Column("publisher", Text, nullable=False)
    published = Column("published", Integer, nullable=False)
    msg = Column("msg", Text, nullable=False)
    until = Column("until", Integer, nullable=True)
    msg_hash = Column("msg_hash", Text, nullable=True)
    previous_hash = Column("previous_hash", Text, nullable=True)

    __table_args__ = (UniqueConstraint('message_id', 'publisher', 'published', 'until', name='_unique_pair'),)

    @classmethod
    def create_with_hash(cls, msg_id, publisher, msg, published, previous_hash=None, until=None):
        t = cls(message_id=msg_id, publisher=publisher, published=published, msg=msg, previous_hash=previous_hash, until=until)
        ret =  cls(message_id=msg_id, publisher=publisher, published=published, msg=msg, until=until, previous_hash=previous_hash, msg_hash=t.calculate_hash())

        return ret

    def calculate_hash(self):
        return sign_content(str(self), self.publisher)

    def __str__(self):
        return f'{self.message_id}-{self.publisher}-{self.published}-{self.msg}-{self.until}-{self.previous_hash}'

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(self.msg_hash) ^ hash(self.message_id)

    def __eq__(self, other):
        
        if self.message_id != other.message_id:
            return False
        return self.msg_hash == other.msg_hash

    @property
    def valid_hash(self):
        return self.msg_hash == self.calculate_hash()
    



    @classmethod
    def from_dict(cls, d):
        return cls(message_id=d['message_id'], publisher=d['publisher'], msg=d['msg'], published=d['published'], until=d['until'], msg_hash=d['msg_hash'], previous_hash=d['previous_hash'])


    def to_dict(self):

        return {
                'message_id': self.message_id,
                'publisher': self.publisher,
                'msg': self.msg,
                'msg_hash': self.msg_hash,
                'previous_hash': self.previous_hash,
                'published': self.published,
                'until': self.until
                }


def convert_timestamp(t):
    return str(datetime.fromtimestamp(t, timezone.utc))



class Aggregator:

    def __init__(self,
            name,
            quart,
            kad,

            max_saved_msgs,
            request_timeout,


            replicate_min_interval,
            replicate_max_interval,
            trim_and_save_interval,
            delete_invalid_entries_interval,
            dht_share_stats_interval,
            dht_announce_interval,
            missing_fixer_interval,
            get_msgs_interval,
            persist_following_interval,

            remove_entry_seconds,

            self_trim
            ):

        self.timeout = request_timeout
        self.name = name
        self.quart = quart
        self.kad = kad
        self.dht_pair = None
        self.comm_pair = None
        self.remove_entry_seconds = remove_entry_seconds
        self.following = []
        self.saved_msgs = defaultdict(lambda: list())

        self.replicate_min_interval = replicate_min_interval
        self.replicate_max_interval = replicate_max_interval
        self.trim_and_save_interval = trim_and_save_interval
        self.delete_invalid_entries_interval = delete_invalid_entries_interval
        self.dht_share_stats_interval = dht_share_stats_interval
        self.dht_announce_interval = dht_announce_interval
        self.missing_fixer_interval = missing_fixer_interval
        self.get_msgs_interval = get_msgs_interval
        self.persist_following_interval = persist_following_interval

        
        self.max_saved_msgs = max_saved_msgs
        self.self_trim = self_trim

        self.inited = False

        self.engine = create_async_engine(f"sqlite+aiosqlite:///{self.name}.db")

        # expire_on_commit=False will prevent attributes from being expired
        # after commit.
        self.session_creator = sessionmaker(
            bind=self.engine, expire_on_commit=False, class_=AsyncSession
        )

        self.lock = asyncio.Lock()



    def save_received_msg(self, msg):

        if not msg.valid_hash:
            log.warning(f'Invalid message received')
            return False

        key = msg.publisher
        orig_size = len(self.saved_msgs[key])
        self.saved_msgs[key].append(msg)
        self.saved_msgs[key] = list(set(self.saved_msgs[key]))

        msg_id = 'ephemeral' if msg.until is not None else f'#{msg.message_id}'
        if orig_size != len(self.saved_msgs[key]):
            log.info(f'Message {msg_id} from {msg.publisher} has been saved')
            return True

        log.info(f'Already had message {msg_id} from {msg.publisher}')
        return False


    async def replicate_msg(self, msg, with_sleep=True):

        if with_sleep:
            await asyncio.sleep(randint(self.replicate_min_interval, self.replicate_max_interval))
        replicas = await self._dht_get_replicas(msg.publisher)


        replicas = sorted(replicas, key=lambda x: x['latest_normal'], reverse=True)

        replicas = replicas[:2]

        if len(replicas) == 0:
            return

        for replica in replicas:
            async with aiohttp.ClientSession(json_serialize=ujson.dumps) as session:
                address = f'http://{replica["host"]}:{replica["port"]}/post_msg'
                log.info(f'Replicating to {replica["signature_owner"]} {address}')
                try:
                    await session.post(address, json=msg.to_dict(), timeout=self.timeout)
                except asyncio.TimeoutError as e:
                    log.error(f'Timed out: when replicating to {replica["signature_owner"]} {address}')
                except aiohttp.client_exceptions.ClientConnectorError:
                    log.error(f'Connection error: when replicating to {replica["signature_owner"]} {address}')
                except Exception:
                    log.error(f'Unknown exception: when replicating to {replica["signature_owner"]} {address}')

    async def received_msg(self, msg):
        log.info('Received a message')
        res = self.save_received_msg(msg)

        if res:
            asyncio.get_event_loop().create_task(self.replicate_msg(msg))


    async def _save_following(self):

        
        while True:
            following = set(self.following[:])

            db_following = set()
            async with self.session_creator() as session:

                res = await session.execute(select(Following))
                db_following = set([x.name for x in res.scalars()])

            
            if following == db_following:
                await asyncio.sleep(self.persist_following_interval)
                continue

            # remove unsubs from db
            unsubs = db_following - following

            if unsubs:
                async with self.session_creator() as session:

                    for unsub in unsubs:
                        log.info(f'Persisting unsub - {unsub}')
                        await session.execute(delete(Following).where(Following.name == unsub))
                        await session.commit()


            # add new subs
            missings = following - db_following

            if missings:
                async with self.session_creator() as session:
                    for missing in missings:
                        log.info(f'Persisting sub - {missing}')
                        session.add(Following(name=missing))
                    await session.commit()

            await asyncio.sleep(self.persist_following_interval)




    async def trim_and_save_msgs_single(self, key, session, cur_time, trim=True, filter_type=GetMsgType.FILTER_EPHEMERAL):

            '''
            stmt = select(func.max(Message.published)).filter_by(publisher = key)
            current_max_co = await session.execute(stmt)
            current_max = current_max_co.first()[0]
            '''

            msgs = await self.get_msgs_by_name(key, current_timestamp=cur_time, filter_type=filter_type)

            # only keep with valid hashes
            msgs = list(set(filter(lambda x: x.valid_hash, msgs)))
            self.saved_msgs[key] = msgs

            for msg in msgs:
                session.add(msg)
            await session.commit()


            if trim:
                cur = sorted(msgs, key=lambda x: x.published, reverse=True)
                cur = cur[:self.max_saved_msgs]
                self.saved_msgs[key] = cur

    async def trim_and_save_msgs(self):
        while True:

            cur_time = await self.get_ntp_time()

            async with self.session_creator() as session:

                # save own messages
                await self.trim_and_save_msgs_single(self.name, session, cur_time, trim=self.self_trim, filter_type = GetMsgType.KEEP_EPHEMERAL)

                #save following messages
                keys = self.following[:]
                for key in keys:
                    await self.trim_and_save_msgs_single(key, session, cur_time)



            await asyncio.sleep(self.trim_and_save_interval)


    async def delete_db_invalid_hash(self, session, name):

        stmt = select(Message).filter_by(publisher=name)
        co = await session.execute(stmt)

        for msg in co.scalars():

            if not msg.valid_hash:
                log.info(f'Deleting message with the following hash - {msg.msg_hash}')
                await session.execute(delete(Message).where(Message.ignored_id == msg.ignored_id))
                await session.commit()


    async def _delete_db_invalid_entries(self):

        while True:
            following = self.following[:]
            async with self.session_creator() as session:
                await self.delete_db_invalid_hash(session, self.name)

                for follow in following:
                    await self.delete_db_invalid_hash(session, follow)



            await asyncio.sleep(self.delete_invalid_entries_interval)



    async def _init(self):

        async with self.lock:
            if self.inited == True:
                return

            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)


            async with self.session_creator() as session:

                # reload my messages

                my_msgs_co = await session.execute(select(Message).filter_by(publisher=self.name))
                self.saved_msgs[self.name] = list(set([x for x in my_msgs_co.scalars().all()]))


                # reload following messages
                r = await session.execute(select(Following))
                self.following = [x.name for x in r.scalars()]

                for following in self.following:
                    r = await session.execute(select(Message).filter_by(publisher=following))
                    self.saved_msgs[following] = list(set([x for x in r.scalars().all()]))


            self.inited = True


    async def async_request_and_update_messages(self, session, address, name, owner):
        
        ret_val = False
        try:
            async with session.get(address, timeout=self.timeout) as res:
                if res.status != 200:
                    return False

                orig_size = len(self.saved_msgs[name])

                self.saved_msgs[name].extend(map(Message.from_dict, ujson.loads(await res.text())))
                self.saved_msgs[name] = list(set(self.saved_msgs[name]))
                ret_val = orig_size != len(self.saved_msgs[name])

        except asyncio.TimeoutError as e:
            log.error(f'Got timeout getting data of {name} from {owner}')
            return False
        except aiohttp.client_exceptions.ClientConnectorError:
            log.error(f'Connection error: getting data {name} from {owner}')
            return False
        except Exception:
            log.error(f'Unknown exception: getting data {name} from {owner}')
            return False

        return ret_val

    async def update_saved_msgs_from_x_data(self, session, name, my_stats, x_data):

        ret_val = False
        if x_data is None:
            return ret_val

        if x_data['latest_normal'] > my_stats['latest_normal']:
            log.info(f'Will get data of {name} from {x_data["signature_owner"]}')
            endpoint = f'http://{x_data["host"]}:{x_data["port"]}'
            address = f'{endpoint}/get_msgs/{name}'
            ret = await self.async_request_and_update_messages(session, address, name, x_data['signature_owner']) 

            if ret:
                log.info(f'Got new messages of {name} from {x_data["signature_owner"]}')

            ret_val = ret or ret_val


        if x_data['num_ephemeral'] > my_stats['num_ephemeral']:
            log.info(f'Will get ephemeral data of {name} from {x_data["signature_owner"]}')
            endpoint = f'http://{x_data["host"]}:{x_data["port"]}'
            address = f'{endpoint}/get_ephemeral/{name}'
            ret = await self.async_request_and_update_messages(session, address, name, x_data['signature_owner']) 

            if ret:
                log.info(f'Got new ephemeral messages of {name} from {x_data["signature_owner"]}')

            ret_val = ret or ret_val


        return ret_val


    async def _get_following_msgs_single(self, session, name):


        my_stats = await self.get_node_msgs_stats(name, None, None)
        source_data = await self.get_dht(f'{name}-source')

        
        res = await self.update_saved_msgs_from_x_data(session, name, my_stats, source_data)

        if res:
            return

        current_time = await self.get_ntp_time()
        replicas_data = await self._dht_get_replicas(name)
        replicas_data = filter(lambda x: (x['posted'] + self.remove_entry_seconds) > current_time, replicas_data)
        replicas_data = sorted(replicas_data, key=lambda x: (x['latest_normal'], x['num_ephemeral']), reverse=True)
        replicas_data = list(replicas_data)


        for replica in replicas_data:
            if await self.update_saved_msgs_from_x_data(session, name, my_stats, replica):
                my_stats = await self.get_node_msgs_stats(name, None, None)



            
    async def _get_following_msgs(self):
        while True:

            if len(self.following) == 0:
                await asyncio.sleep(self.get_msgs_interval)
                continue


            following = self.following[:]
            async with aiohttp.ClientSession() as session:
                for pair in following:
                    await self._get_following_msgs_single(session, pair)


            await asyncio.sleep(self.get_msgs_interval)


    def unfollow(self, name):
        if name in self.following:
            self.following.remove(name)
        if name in self.saved_msgs:
            del self.saved_msgs[name]
	
	    

    def follow(self, name):
        self.following.append(name)
        self.following = list(set(self.following))

    async def get_msgs_by_name(self, name, current_timestamp=0, filter_type=GetMsgType.FILTER_EPHEMERAL):

        if filter_type == GetMsgType.FILTER_EPHEMERAL:
            current_timestamp = await self.get_ntp_time()

        msgs = filter(partial(filter_ephemeral_msg, filter_type, current_timestamp), self.saved_msgs[name])
        msgs = filter(lambda x: x.valid_hash, msgs)
        return list(msgs)


    def has_hole_single(self, msgs):

        if len(msgs) == 0:
            return (False, None)

        msgs = filter(lambda x: x.until is None, msgs)
        sorted_msgs = list(sorted(msgs, key=lambda x: x.message_id))

        if len(sorted_msgs) == 0:
            return (False, None)


        my_msgs_ids = list(map(lambda x: x.message_id, sorted_msgs)) 
        supposed_ids = list(range(sorted_msgs[0].message_id, sorted_msgs[-1].message_id+1))

        missing = set(supposed_ids) - set(my_msgs_ids)
        if len(missing) > 0:
            return (True, missing)

        if len(sorted_msgs) == 1:
            return (False, None)

        invalid_hashes = any(filter(lambda x: x[0].msg_hash != x[1].previous_hash, zip(sorted_msgs[:-1], sorted_msgs[1:])))


        if invalid_hashes:
            return (True, None)
        
        return (False, None)


    async def get_missing_msgs(self, name):
        msgs = await self.get_msgs_by_name(name, filter_type=GetMsgType.REMOVE_EPHEMERAL)

        r = self.has_hole_single(msgs)

        if r[0] and r[1] is not None:
            return r[1]
        return []


    def should_query(self, missing, x_data):
            messages_in_peer = set(range(x_data['first_normal'], x_data['latest_normal']+1))
            return len(missing.intersection(messages_in_peer)) > 0

    async def _hole_filler(self):

        while True:

            following = self.following[:]

            current_timestamp = await self.get_ntp_time()

            async with aiohttp.ClientSession() as session:
                for follow in following:

                    missing = await self.get_missing_msgs(follow)

                    if missing:

                        log.warning(f'Missing messages from: {follow}')

                        source = await self.get_dht(f'{follow}-source')
                        if source is not None and self.should_query(missing, source):
                            endpoint = f'http://{source["host"]}:{source["port"]}'
                            address = f'{endpoint}/get_msgs/{follow}'
                            if await self.async_request_and_update_messages(session, address, follow, source['signature_owner']):
                                log.info(f'Succesfully got data of {follow} from {source["signature_owner"]}')
                                missing = await self.get_missing_msgs(follow)

                        if len(missing) == 0:
                            break

                        replicas = await self._dht_get_replicas(follow)
                        for replica in replicas:

                            should_query = self.should_query(missing, replica)

                            endpoint = f'http://{replica["host"]}:{replica["port"]}'
                            address = f'{endpoint}/get_msgs/{follow}'
                            if should_query:
                                if await self.async_request_and_update_messages(session, address, follow, replica['signature_owner']):
                                    log.info(f'Succesfully got data of {follow} from {source["signature_owner"]}')
                                    missing = await self.get_missing_msgs(follow)


            await asyncio.sleep(self.missing_fixer_interval)


    async def get_msgs(self, keep_self_ephemeral=False):

        msgs = []
        holes = []
        following = self.following[:]

        filter_type = GetMsgType.FILTER_EPHEMERAL
        if keep_self_ephemeral:
            filter_type = GetMsgType.KEEP_EPHEMERAL


        my_msgs = await self.get_msgs_by_name(self.name, filter_type=filter_type)

        if self.has_hole_single(my_msgs)[0]:
            holes.append(self.name)

        msgs.extend(my_msgs)

        for k in following:
            n = await self.get_msgs_by_name(k)

            if self.has_hole_single(n)[0]:
                holes.append(k)
            msgs.extend(n)

        
        return (sorted(msgs, key=lambda x: (x.published, x.message_id), reverse=True), holes)

    async def add_msg(self, msg, duration=None):

        async with self.lock:
            t = await g_Agg.get_ntp_time()

            until = None
            previous_hash = None
            if duration is not None:
                until = t + duration
                msg_id = randint(1, 400)
            else:

                my_msgs = await self.get_msgs_by_name(self.name, current_timestamp=0, filter_type=GetMsgType.REMOVE_EPHEMERAL)
                my_msgs = list(sorted(my_msgs, key=lambda x: x.message_id, reverse=True))

                msg_id = 1
                if my_msgs:
                    msg_id += my_msgs[0].message_id
                    previous_hash = my_msgs[0].msg_hash


            pub_msg = Message.create_with_hash(msg_id=msg_id, publisher=self.name, msg=msg, published=t, previous_hash=previous_hash, until=until)
            self.saved_msgs[self.name].append(pub_msg)

        asyncio.get_event_loop().create_task(self.replicate_msg(pub_msg, with_sleep=False))

    def _get_ntp_time(self):
        while True:
            client = ntplib.NTPClient()
            try:
                return int(client.request('pool.ntp.org', version=3).tx_time)
            except Exception:
                pass

    async def get_ntp_time(self):
        return int(datetime.timestamp(datetime.utcnow()))
        #return await asyncio.get_event_loop().run_in_executor(None, self._get_ntp_time)


    def validate_get_dht(self, key, value):

        try:
            r = ujson.loads(value)

            if isinstance(r, dict):
                res = SignedDict.is_valid(r)

                if res == False:
                    log.warning(f'Wrong signature for key - {key}')

                return (res, r)

            if isinstance(r, list):

                res = list(filter(lambda x: SignedDict.is_valid(x), r))

                if len(r) != len(res):
                    log.warning(f'Wrong signature in some items for key - {key}')

                return (len(res) > 0, res)

            raise ValueError(f'unknown type {type(r)}')

        except:
            log.error('EXCEPTION')
            return (False, None)

    async def get_dht(self, key):
        try:
            ret = await self.kad.get(key)

            if ret is None:
                return None

            vad_res = self.validate_get_dht(key, ret)

            if vad_res[0]:
                return vad_res[1]
            return None
        except Exception as e:
            log.error(f'Excepcao {e}')
            return None

    async def set_dht(self, key, value):

        if not isinstance(value, dict) and not isinstance(value, list):
            raise ValueError(f"can't set {type(value)}({value}) in the DHT")

        
        if isinstance(value, dict):
            if 'signature' not in value:
                raise ValueError('can only set signed dicts in the DHT')

        if isinstance(value, list):
            if any(filter(lambda x: 'signature' not in x, value)):
                raise ValueError('can only set lists of signed dicts in the DHT')

        value = ujson.dumps(value)
        try:
            return await self.kad.set(key, value)
        except Exception:
            return False

    async def _dht_get_replicas(self, name):
            res = await self.get_dht(f'{name}-replicas')

            if res is None:
                res = []
            else:
                res = list(filter(lambda x: x['signature_owner'] != self.name, res))

            return res

    async def _dht_get_nodes(self):
            res = await self.get_dht('nodes')

            if res is None:
                res = []
            else:
                res = list(filter(lambda x: x['name'] != self.name, res))

            return res


    async def get_latest_msg_id_of_node_msg(self, name):

        msgs = await self.get_msgs_by_name(name, filter_type=GetMsgType.REMOVE_EPHEMERAL)
        if len(msgs) == 0:
            return 0

        return max(map(lambda x: x.message_id, msgs))

    async def get_latest_timestamp_of_node_msg(self, name):

        msgs = await self.get_msgs_by_name(name)
        if len(msgs) == 0:
            return 0

        return max(map(lambda x: x.published, msgs))



    async def get_node_msgs_stats(self, name, host, port):
        msgs = await self.get_msgs_by_name(name)


        if len(msgs) == 0:
            return {
                    'host':host,
                    'port':port,
                    'num_ephemeral': 0,
                    'latest_normal': 0,
                    'first_normal': 0,
                    }

        normal = list(sorted(filter(lambda x: x.until is None, msgs), key=lambda x: x.message_id))
        ephemeral = list(filter(lambda x: x.until is not None, msgs))

        
        if len(normal) == 0:
            latest_normal = 0
            first_normal = 0
        else:
            latest_normal = normal[-1].message_id
            first_normal = normal[0].message_id


        return {
                'host':host,
                'port':port,
                'num_ephemeral': len(ephemeral),
                'latest_normal': latest_normal,
                'first_normal': first_normal,
                }


    async def _dht_announce_presence_source_and_message_min_max(self, current_timestamp):

        stats = await self.get_node_msgs_stats(self.name, self.comm_pair[0], self.comm_pair[1])
        await self.set_dht(f'{self.name}-source', SignedDict(posted=current_timestamp, **stats).signed)



    async def _dht_announce_presence(self):
        while True:

            current_timestamp = await self.get_ntp_time()
            await self._dht_announce_presence_source_and_message_min_max(current_timestamp)

            if current_timestamp == 0:
                asyncio.sleep(self.dht_announce_interval)
                continue

            res = await self._dht_get_nodes()

            res = filter(lambda x: (x['posted'] + self.remove_entry_seconds) > current_timestamp, res)
            res = list(res)

            sd = SignedDict(name=self.name, posted=current_timestamp)
            res.append(sd.signed)


            await self.set_dht('nodes', list(res))
            await asyncio.sleep(self.dht_announce_interval)

    async def _dht_share_stats(self):

        while True:
            current_timestamp = await self.get_ntp_time()
            following = self.following[:]

            
            for follow in following:
                replicas = await self._dht_get_replicas(follow)
                replicas = filter(lambda x: (x['posted'] + self.remove_entry_seconds) > current_timestamp, replicas) 

                replicas = list(replicas)
                stats = await self.get_node_msgs_stats(follow, self.comm_pair[0], self.comm_pair[1])
                replicas.append(SignedDict(posted=current_timestamp, **stats).signed)

                await self.set_dht(f'{follow}-replicas', replicas)

            await asyncio.sleep(self.dht_share_stats_interval)


    async def run_quart(self, host, port):
        await self._init()
        self.comm_pair = (host, port)

        reload = False
        if self.name == 'node1':
            reload = True
        return await self.quart.run_task(host, port, debug=reload, use_reloader=reload)
    
    async def run_kad(self, host, port, bootstrap=None):

        await self._init()

        self.dht_pair = (host, port)
        log.info(f'DHT listening at {self.dht_pair}')

        await self.kad.listen(port, interface=host)
        asyncio.get_event_loop().create_task(self.trim_and_save_msgs())
        asyncio.get_event_loop().create_task(self._dht_announce_presence())
        asyncio.get_event_loop().create_task(self._get_following_msgs())
        asyncio.get_event_loop().create_task(self._save_following())
        asyncio.get_event_loop().create_task(self._dht_share_stats())
        asyncio.get_event_loop().create_task(self._hole_filler())
        asyncio.get_event_loop().create_task(self._delete_db_invalid_entries())

        if bootstrap is not None:
            log.info(f'Will bootstrap at {bootstrap}')

        if bootstrap is not None:
            await self.kad.bootstrap([bootstrap])

        if self.name != 'meme':
            return


@app.route('/thing')
async def test_params():
    k = g_Agg.saved_msgs[g_Agg.name][-1]
    return f'thing {k} {k.msg_hash} {k.calculate_hash()} {k.valid_hash}'


@app.route('/post_msg', methods=['POST'])
async def post_msg():
    data = await request.get_data()
    msg = Message.from_dict(ujson.loads(data))
    await g_Agg.received_msg(msg)

    return ''
    

    

@app.route('/get_ephemeral/<node_name>')
async def get_ephemeral_msgs(node_name):
    l = map(lambda x: x.to_dict(), await g_Agg.get_msgs_by_name(node_name, filter_type=GetMsgType.ONLY_EPHEMERAL))
    l = list(l)
    return ujson.dumps(l)

@app.route('/get_msgs/<node_name>')
async def get_msgs(node_name):
    l = map(lambda x: x.to_dict(), await g_Agg.get_msgs_by_name(node_name))
    l = list(l)
    return ujson.dumps(l)

@app.route("/submit_message", methods=['POST'])
async def submit_message():
    form = await request.form

    form = form.to_dict()

    duration = None
    if 'ephemeral' in form:
        duration = int(form['duration']) if form['ephemeral'] == 'on' else None

    await g_Agg.add_msg(form['msg'], duration)
    return redirect(url_for('hello'))

@app.route("/follow/<name>")
async def follow(name):
    g_Agg.follow(name)
    return redirect(url_for('hello'))

@app.route("/unfollow/<name>")
async def unfollow(name):
    g_Agg.unfollow(name)
    return redirect(url_for('hello'))

@app.route("/")
async def hello():

    cur_time = await g_Agg.get_ntp_time()
    following = g_Agg.following[:]

    nodes = await g_Agg._dht_get_nodes()
    nodes = list(filter(lambda x: x['name'] != g_Agg.name and x['name'] not in g_Agg.following, nodes))


    msgs,holes = await g_Agg.get_msgs(keep_self_ephemeral=True)

    hole = ''
    if len(holes) > 0:
        hole = ','.join(holes)

    return await render_template("index.html", name=g_Agg.name, cur_time=cur_time, nodes=nodes, msgs=msgs, hole=hole, timestamp_converter=convert_timestamp, following=following)

async def will_die():
    await asyncio.sleep(1)
    raise KeyboardInterrupt()
    
@app.route("/die")
async def die():
    asyncio.get_event_loop().create_task(will_die())
    return 'dead'



loop = asyncio.get_event_loop()
loop.set_debug(True)


def get_free_port_udp(host):

    with open('current_udp.txt', 'a+') as fp:
        fp.seek(0,0)
        res = fp.readline()
        if res == '':
            res = 7000
        else:
            res = int(res)

        fp.seek(0,0)
        fp.truncate()
        fp.write(f'{res+1}\n')

    return res

def get_free_port_tcp(host):

    for i in range(5000, 6000):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind((host, i))
            return i
        except Exception:
            continue
        finally:
            s.close()
    raise ValueError('no free port')

def get_free_port(host, udp=False):

    if udp:
        return get_free_port_udp(host)
    return get_free_port_tcp(host)



def main():

    global g_Agg
    args = parse_arguments(new_arguments=[

        ('--max_saved_msgs', int, False, None, 5),
        ('--request_timeout', int, False, None, 5),

        ('--replicate_min_interval', int, False, None, 5),
        ('--replicate_max_interval', int, False, None, 10),
        ('--trim_and_save_interval', int, False, None, 5),
        ('--delete_invalid_entries_interval', int, False, None, 10),
        ('--dht_share_stats_interval', int, False, None, 10),
        ('--dht_announce_interval', int, False, None, 10),
        ('--missing_fixer_interval', int, False, None, 5),
        ('--get_msgs_interval', int, False, None, 5),

        ('--persist_following_interval', int, False, None, 10),
        ('--remove_entry_seconds', int, False, None, 40),
        
        ('--self_trim', bool, False, None, False),

        ])
    server = Server()

    g_Agg = Aggregator(args.name, app, server,

            max_saved_msgs = args.max_saved_msgs,
            request_timeout = args.request_timeout,

            replicate_min_interval = args.replicate_min_interval,
            replicate_max_interval = args.replicate_max_interval,
            trim_and_save_interval = args.trim_and_save_interval,
            delete_invalid_entries_interval = args.delete_invalid_entries_interval,
            dht_share_stats_interval = args.dht_share_stats_interval,
            dht_announce_interval = args.dht_announce_interval,
            missing_fixer_interval = args.missing_fixer_interval,
            get_msgs_interval = args.get_msgs_interval,
            persist_following_interval = args.persist_following_interval,

            remove_entry_seconds = args.remove_entry_seconds,

            self_trim = args.self_trim
            )

    port_dht = get_free_port(args.host_dht, True) if args.port_dht is None else args.port_dht
    port_comm = get_free_port(args.host_comm) if args.port_comm is None else args.port_comm


    bootstrap_pair = None if args.port_boots is None else (args.host_boots, args.port_boots)

    loop.create_task(g_Agg.run_kad(args.host_dht, port_dht, bootstrap_pair))
    loop.create_task(g_Agg.run_quart(args.host_comm, port_comm))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            server.stop()
            loop.close()
        except:
            pass


if __name__ == '__main__':
    main()


