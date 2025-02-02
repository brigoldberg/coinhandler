# utils.py

import argparse
import toml

def parse_config(fn, section):
    """Read TOML configuration file and return dict"""
    with open(fn) as fh:
        data = toml.load(fh)
        data = data[section]
    return data

def cli_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', dest='cfg_fn', action='store', required=True)
    return parser.parse_args()