import sys
import string
import random
import requests

def generate_random_string(length):
    return ''.join(random.choice(string.ascii_letters) for i in range(length))


def big_generator(num_entries, length):
    r = set()
    while len(r) != num_entries:
        r.add(generate_random_string(length))
    return r


def main():
    port = int(sys.argv[1])
    num_msgs = int(sys.argv[2])

    print('will generate...')
    msgs = big_generator(num_msgs, 128)
    print('generated')

    address = f'http://127.0.0.1:{port}/submit_message'
    with requests.Session() as session:
        for i, msg in enumerate(msgs):
            session.post(address, data={'msg': msg})
            print(f'Done {i+1}/{num_msgs}\r', end='')
        print('Done')



if __name__ == '__main__':
    main()
