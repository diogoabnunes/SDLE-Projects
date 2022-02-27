import subscriber
import publisher
import string
import random
import multiprocessing
import sys
import time


def generate_random_string(length):
    return ''.join(random.choice(string.ascii_letters) for i in range(length))


def big_generator(num_entries, length):

    r = set()

    while len(r) != num_entries:
        r.add(generate_random_string(length))
    return r

def publisher_process(number, q3):

    pub = publisher.Publisher()
    pub.start('127.0.0.1', '7070', 'connect')
    msgs = big_generator(number, 10)

    q3.get()
    for msg in msgs:
        pub.put('test', msg)
    print('acabei')


def subscriber_process(sub_id, number, q, q2, equality):
    msgs = set()
    sub = subscriber.Subscriber(sub_id)
    sub.start('127.0.0.1', '6969', 'connect')

    sub.subscribe('test')
    q.put(1)
    q2.get()



    read = 0
    while read != number:
        res = sub.get('test', 50)
        if res is not None:
            read += len(res)
            if equality:
                msgs.update(res)

    if equality and len(msgs) != number:
        raise ValueError("oops")

def main():


    num_msgs = int(sys.argv[1])
    num_subs = int(sys.argv[2])

    equality = False
    if len(sys.argv) == 4:
        if sys.argv[3] != 'equal':
            print("The message check parameter must be 'equal'")
            return
        equality = True

    sub_ids = big_generator(num_subs, 20)

    q, q2, q3 = multiprocessing.Queue(), multiprocessing.Queue(), multiprocessing.Queue()
    publisher_p = multiprocessing.Process(target=publisher_process, args=(num_msgs, q3))


    sub_processes = [multiprocessing.Process(target=subscriber_process, args=(sub, num_msgs, q, q2, equality)) for sub in sub_ids]



    print('Ready to start')
    publisher_p.start()
    for entry in sub_processes:
        entry.start()

    #wait for subscribe()
    for i in range(num_subs):
        q.get()

    #let publisher put()
    q3.put(1)
    #let subscribers get()
    for _ in range(num_subs):
        q2.put(1)

    start = time.time()
    publisher_p.join()
    for entry in sub_processes:
        entry.join()

    print(f'Finished in {time.time()-start} seconds')
    return



if __name__ == '__main__':
    main()
