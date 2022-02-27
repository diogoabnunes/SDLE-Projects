import argparse


def parse_arguments(disable_arguments=set(), new_arguments=[]):

    parser = argparse.ArgumentParser()

    default_args = {
        "host": ("--host", str, True, None, None),
        "port": ("--port", str, True, None, None),
        "connect_type": ("--connect_type", str, True, ["connect", "bind"], "connect"),
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
