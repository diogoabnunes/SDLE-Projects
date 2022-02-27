import argparse


def parse_arguments(disable_arguments=set(), new_arguments=[]):

    parser = argparse.ArgumentParser()

    default_args = {
        "host_dht": ("--host_dht", str, False, None, '127.0.0.1'),
        "port_dht": ("--port_dht", int, False, None, None),
        "host_comm": ("--host_comm", str, False, None, '127.0.0.1'),
        "port_comm": ("--port_comm", str, False, None, None),
        "name": ("--name", str, True, None, None),
        "host_boots": ("--host_boots", str, False, None, '127.0.0.1'),
        "port_boots": ("--port_boots", int, False, None, None),
        #"connect_type": ("--connect_type", str, True, ["connect", "bind"], "connect"),
    }

    for key, value in default_args.items():

        if key in disable_arguments:
            continue
        parser.add_argument(
            value[0],
            type=value[1],
            required=value[2],
            choices=value[3],
            default=value[4],
        )

    for value in new_arguments:
        parser.add_argument(
            value[0],
            type=value[1],
            required=value[2],
            choices=value[3],
            default=value[4],
        )

    return parser.parse_args()
