# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import sys
import os
import time
import datetime
import argparse
import traceback
import signal
from .utils import print_object, get_rich, human_format_object
from .logger import get_logger
from .taskers.core import CoreTasker

def beautify_print(*args, **kwargs):
    rich = get_rich()
    if rich:
        rich.get_console().print(markup=False, *args, **kwargs)
    else:
        print_object(*args, **kwargs)

def warp_database_logging(tasker):
    def commit_warper(database, builder, func):
        def _(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
            finally:
                database_verbose = database.verbose()
                builder_verbose = builder.verbose()
                beautify_print("%s %s %s -> %s<%s> %.2fms" % (datetime.datetime.now(), database.__class__.__name__,
                                                          database_verbose, builder.__class__.__name__, builder.name,
                                                          (time.time() - start_time) * 1000))
                if builder_verbose:
                    if isinstance(builder_verbose, tuple):
                        for v in builder_verbose:
                            beautify_print(v)
                    else:
                        beautify_print(builder_verbose)
                    print()
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
        if "action" in argument:
            kwargs["action"] = argument["action"]
        if "nargs" in argument:
            kwargs["nargs"] = argument["nargs"]
        if "const" in argument:
            kwargs["const"] = argument["const"]
        if "choices" in argument and isinstance(argument["choices"], list):
            kwargs["choices"] = argument["choices"]
        if argument["name"] not in register_aps:
            if "short" in argument and len(argument["short"]) == 1:
                register_aps[argument["name"]] = ap.add_argument('-%s' % argument["short"], '--%s' % argument["name"],
                                                                 dest=("%s@%s") % (tasker.name, argument["name"]), **kwargs)
            else:
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

def fix_print_outputer(tasker, register_aps, arguments):
    if "@output" not in register_aps:
        return
    if "@output" not in arguments or arguments["@output"] != "-":
        return
    for database in tasker.config["databases"]:
        if database["name"] == "-":
            return
    tasker.config["databases"].append({
        "name": "-",
        "driver": "textline",
        "format": "print"
    })
    primary_key = "::".join((register_aps["@output"].default or "").split("::")[1:])
    arguments["@output"] = "&.-.&1::" + primary_key

def show_tasker(tasker):
    config = {key: value for key, value in tasker.config.items()}
    config["schema"] = tasker.schema
    beautify_print(human_format_object(config))

def show_dependency_tasker(tasker, dependency_taskers):
    for dependency_tasker in dependency_taskers:
        show_dependency_tasker(*dependency_tasker)
    show_tasker(tasker)

def main():
    if len(sys.argv) < 2:
        print("usage: syncany [-h] json|yaml")
        print("syncany error: too few arguments")
        exit(2)

    if not sys.argv[1].endswith("json") and not sys.argv[1].endswith("yaml") \
            and not sys.argv[1].startswith("http"):
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
        ap.add_argument("-s", '--@show', dest='@show', nargs='?', const=True, metavar=False,
                        default=False, type=bool, help='show compile config (defualt: False)')
        ap.add_argument("-v", '--@verbose', dest='@verbose', nargs='?', const=True, metavar=False,
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
            if "action" in argument:
                kwargs["action"] = argument["action"]
            if "nargs" in argument:
                kwargs["nargs"] = argument["nargs"]
            if "const" in argument:
                kwargs["const"] = argument["const"]
            if "choices" in argument and isinstance(argument["choices"], list):
                kwargs["choices"] = argument["choices"]
            if "short" in argument and len(argument["short"]) == 1:
                register_aps[argument["name"]] = ap.add_argument('-%s' % argument["short"], '--%s' % argument["name"],
                                                                 dest=argument["name"], **kwargs)
            else:
                register_aps[argument["name"]] = ap.add_argument('--%s' % argument["name"], dest=argument["name"], **kwargs)

        dependency_taskers = []
        for filename in tasker.get_dependency():
            dependency_taskers.append(load_dependency(filename, ap, register_aps))

        ap_arguments = ap.parse_args()
        arguments = {key.lower(): value for key, value in os.environ.items()}
        arguments.update(ap_arguments.__dict__)

        fix_print_outputer(tasker, register_aps, arguments)
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