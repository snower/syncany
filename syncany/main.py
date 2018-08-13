# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import argparse
from .taskers.json_tasker import JsonTasker

def main():
    parser = argparse.ArgumentParser(description='syncany')
    parser.add_argument("json", type=str, nargs=argparse.OPTIONAL, help="json filename")
    args = parser.parse_args()

    tasker = JsonTasker(args.json)
    tasker.run()

if __name__ == "__main__":
    main()