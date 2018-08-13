# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import sys
from .taskers.json_tasker import JsonTasker

def main():
    if len(sys.argv) < 2:
        print("usage: syncany [-h] json")
        print("syncany: error: too few arguments")
        exit()

    if not sys.argv[1].endswith("json"):
        print("usage: syncany [-h] json")
        print("syncany: error: require json file")
        exit()

    tasker = JsonTasker(sys.argv[1])
    tasker.run()

if __name__ == "__main__":
    main()