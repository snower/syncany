# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import argparse
from .taskers.json_tasker import JsonTasker

def main():
    parser = argparse.ArgumentParser(description='syncany')
    parser.add_argument("json", type=str, nargs=1, help="json filename")
    args = parser.parse_args()

    tasker = JsonTasker(args.json[0])
    tasker.run()

if __name__ == "__main__":
    main()