# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import sys
import os
import time
import argparse
import traceback
import signal
from .logger import get_logger
from .taskers.core import CoreTasker

def warp_database_logging(tasker):
    def commit_warper(database, builder, func):
        def _(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
            finally:
                database_verbose = database.verbose()
                builder_verbose = builder.verbose()
                if builder_verbose:
                    get_logger().info("%s %s -> %s %.2fms\n%s\n", database.__class__.__name__, database_verbose,
                                      builder.__class__.__name__, (time.time() - start_time) * 1000,
                                      builder_verbose)
                else:
                    get_logger().info("%s %s -> %s %.2fms", database.__class__.__name__, database_verbose, builder.__class__.__name__,
                                      (time.time() - start_time) * 1000)
            return result
        return _

    def builder_warper(database, func):
        def _(*args, **kwargs):
            builder = func(*args, **kwargs)
            builder.commit = commit_warper(database, builder, builder.commit)
            return builder
        return _

    for name, database in list(tasker.databases.items()):
        database.query = builder_warper(database, database.query)
        database.insert = builder_warper(database, database.insert)
        database.update = builder_warper(database, database.update)
        database.delete = builder_warper(database, database.delete)

def load_dependency(filename, ap, register_aps):
    tasker = CoreTasker(filename)
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
    for filename in tasker.get_dependency():
        dependency_taskers.append(load_dependency(filename, ap, register_aps))
    return (tasker, dependency_taskers)

def compile_dependency(arguments, tasker, dependency_taskers):
    for dependency_tasker in dependency_taskers:
        compile_dependency(arguments, *dependency_tasker)
    kn, knl = (tasker.name + "@"), len(tasker.name + "@")
    arguments = {key[knl:]: value for key, value in arguments.items() if key[:knl] == kn}
    tasker.compile(arguments)
    if "@verbose" in arguments and arguments["@verbose"]:
        warp_database_logging(tasker)

def run_dependency(tasker, dependency_taskers):
    dependency_statistics = []
    for dependency_tasker in dependency_taskers:
        dependency_statistics.append(run_dependency(*dependency_tasker))
    statistics = tasker.run()
    return statistics, dependency_statistics

def show_tasker(tasker):
    config = {key: value for key, value in tasker.config.items()}
    config["schema"] = tasker.schema
    def show_value(value, indent):
        if isinstance(value, dict):
            if not value:
                return print("{}", end="")

            print("{")
            keys = sorted(list(value.keys()))
            for i in range(len(keys)):
                print(indent, end="")
                show_value(keys[i], indent + "    ")
                print(": ", end="")
                show_value(value[keys[i]], indent + "    ")
                print("," if i < len(keys) - 1 else "")
            print(indent[:-4] + "}", end="")
        elif isinstance(value, (tuple, set, list)):
            print("[", end="")
            for i in range(len(value)):
                if isinstance(value[i], dict):
                    print("\n" + indent, end="")
                    show_value(value[i], indent + "    ")
                    print(("," + indent) if i < len(value) - 1 else ("\n" + indent[:-4]), end="")
                else:
                    show_value(value[i], indent)
                    if i < len(value) - 1:
                        print(", ", end="")
            print("]", end="")
        elif isinstance(value, str):
            print('"%s"' % value, end="")
        else:
            print(value, end="")
    try:
        import rich
        rich.print(config)
    except ImportError:
        show_value(config, "    ")
        print()

def show_dependency_tasker(tasker, dependency_taskers):
    for dependency_tasker in dependency_taskers:
        show_dependency_tasker(*dependency_tasker)
    show_tasker(tasker)

def main():
    if len(sys.argv) < 2:
        print("usage: syncany [-h] json|yaml")
        print("syncany error: too few arguments")
        exit(2)

    if not sys.argv[1].endswith("json") and not sys.argv[1].endswith("yaml"):
        print("usage: syncany [-h] json|yaml")
        print("syncany error: require json or yaml file")
        exit(2)

    try:
        tasker = CoreTasker(sys.argv[1])
        signal.signal(signal.SIGHUP, lambda signum, frame: tasker.terminate())
        signal.signal(signal.SIGTERM, lambda signum, frame: tasker.terminate())

        arguments = tasker.load()

        if "description" in tasker.config and tasker.config["description"]:
            description = "syncany %s\r\n%s" % (tasker.name, tasker.config["description"])
        else:
            description = 'syncany %s' % tasker.name
        ap = argparse.ArgumentParser(description=description)
        ap.add_argument("filename", type=str, help="json|yaml filename")
        ap.add_argument('--@show', dest='@show', nargs='?', const=True, metavar=False,
                        default=False, type=bool, help='show compile config (defualt: False)')
        ap.add_argument('--@verbose', dest='@verbose', nargs='?', const=True, metavar=False,
                        default=False, type=bool, help='show detail info (defualt: False)')

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
        for filename in tasker.get_dependency():
            dependency_taskers.append(load_dependency(filename, ap, register_aps))

        ap_arguments = ap.parse_args()
        arguments = {key.lower(): value for key, value in os.environ.items()}
        arguments.update(ap_arguments.__dict__)

        for dependency_tasker in dependency_taskers:
            compile_dependency(arguments, *dependency_tasker)
        tasker.compile(arguments)

        if "@show" in arguments and arguments["@show"]:
            for dependency_tasker in dependency_taskers:
                show_dependency_tasker(*dependency_tasker)
            return show_tasker(tasker)
        if "@verbose" in arguments and arguments["@verbose"]:
            warp_database_logging(tasker)

        for dependency_tasker in dependency_taskers:
            run_dependency(*dependency_tasker)
        tasker.run()
    except SystemError:
        get_logger().error("signal exited")
        exit(130)
    except KeyboardInterrupt:
        get_logger().error("Crtl+C exited")
        exit(130)
    except Exception as e:
        get_logger().error("%s\n%s", e, traceback.format_exc())
        exit(1)
    exit(0)

if __name__ == "__main__":
    main()