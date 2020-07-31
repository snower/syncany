# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import sys
import argparse
import traceback
from .logger import get_logger
from .taskers.json_tasker import JsonTasker

def load_dependency(json_filename, ap, register_aps):
    tasker = JsonTasker(json_filename)
    arguments = tasker.load()

    for argument in arguments:
        kwargs = {}
        if "type" in argument:
            kwargs["type"] = argument["type"]
        if "default" not in argument:
            kwargs["required"] = True
        else:
            kwargs["default"] = kwargs["metavar"] = argument["default"]
        kwargs["help"] = argument.get("help", "")
        if argument["name"] not in register_aps:
            register_aps[argument["name"]] = ap.add_argument('--%s' % argument["name"],
                                                             dest=("%s@%s") % (tasker.name, argument["name"]), **kwargs)
        else:
            register_aps[argument["name"]] = ap.add_argument('--%s@%s' % (tasker.name, argument["name"]),
                                                             dest=("%s@%s") % (tasker.name, argument["name"]), **kwargs)

    dependency_taskers = []
    for json_filename in tasker.get_dependency():
        dependency_taskers.append(load_dependency(json_filename, ap, register_aps))
    return (tasker, dependency_taskers)

def compile_dependency(arguments, tasker, dependency_taskers):
    for dependency_tasker in dependency_taskers:
        compile_dependency(arguments, *dependency_tasker)
    kn, knl = (tasker.name + "@"), len(tasker.name + "@")
    arguments = {key[knl:]: value for key, value in arguments.items() if key[:knl] == kn}
    tasker.compile(arguments)

def run_dependency(tasker, dependency_taskers):
    dependency_statistics = []
    for dependency_tasker in dependency_taskers:
        dependency_statistics.append(run_dependency(*dependency_tasker))
    statistics = tasker.run()
    return statistics, dependency_statistics

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

        if "description" in tasker.config and tasker.config["description"]:
            description = "syncany %s\r\n%s" % (tasker.name, tasker.config["description"])
        else:
            description = 'syncany %s' % tasker.name
        ap = argparse.ArgumentParser(description=description)
        ap.add_argument("json", type=str, help="json filename")

        register_aps = {}
        for argument in arguments:
            kwargs = {}
            if "type" in argument:
                kwargs["type"] = argument["type"]
            if "default" not in argument:
                kwargs["required"] = True
            else:
                kwargs["default"] = kwargs["metavar"] = argument["default"]
            kwargs["help"] = argument.get("help", "")
            register_aps[argument["name"]] = ap.add_argument('--%s' % argument["name"], dest=argument["name"], **kwargs)

        dependency_taskers = []
        for json_filename in tasker.get_dependency():
            dependency_taskers.append(load_dependency(json_filename, ap, register_aps))

        arguments = ap.parse_args()
        arguments = arguments.__dict__

        for dependency_tasker in dependency_taskers:
            compile_dependency(arguments, *dependency_tasker)
        tasker.compile(arguments)
        for dependency_tasker in dependency_taskers:
            run_dependency(*dependency_tasker)
        tasker.run()
    except KeyboardInterrupt:
        get_logger().error("Crtl+C exited")
        exit(130)
    except Exception as e:
        get_logger().error("%s\n%s", e, traceback.format_exc())
        exit(1)
    exit(0)

if __name__ == "__main__":
    main()