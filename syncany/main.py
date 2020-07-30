# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import sys
import argparse
import traceback
import logging
from .taskers.json_tasker import JsonTasker

def main():
    if len(sys.argv) < 2:
        print("usage: syncany [-h] json")
        print("syncany error: too few arguments")
        exit(2)

    if not sys.argv[1].endswith("json"):
        print("usage: syncany [-h] json")
        print("syncany error: require json file")
        exit(2)

    try:
        tasker = JsonTasker(sys.argv[1])
        arguments = tasker.load()

        ap = argparse.ArgumentParser(description='syncany %s' % tasker.name)
        ap.add_argument("json", type=str, help="json filename")
        for argument in arguments:
            kwargs = {}
            if "type" in argument:
                kwargs["type"] = argument["type"]
            if "default" not in argument:
                kwargs["required"] = True
            else:
                kwargs["default"] = argument["default"]
            kwargs["help"] = argument.get("help", "")
            ap.add_argument('--%s' % argument["name"], dest=argument["name"], **kwargs)
        arguments = ap.parse_args()

        tasker.compile(arguments.__dict__)
        tasker.run()
    except KeyboardInterrupt:
        logging.error("Crtl+C exited")
        exit(130)
    except Exception as e:
        logging.error("%s\n%s", e, traceback.format_exc())
        exit(1)
    exit(0)

if __name__ == "__main__":
    main()