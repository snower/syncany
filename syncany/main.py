# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import sys
from .taskers.json_tasker import JsonTasker

def main():
    tasker = JsonTasker(sys.argv[1])
    tasker.run()

if __name__ == "__main__":
    main()