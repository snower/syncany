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
from .utils import print_object, get_rich, human_format_object, human_repr_object
from .logger import get_logger
from .taskers.manager import TaskerManager
from .taskers.core import CoreTasker
from .database.database import DatabaseManager

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

    def cache_do_warper(database, builder, func, name):
        def _(*args, **kwargs):
            start_time, result = time.time(), None
            try:
                result = func(*args, **kwargs)
            finally:
                database_verbose = database.verbose()
                beautify_print("%s %s %s -> %s::%s<%s> %.2fms" % (datetime.datetime.now(), database.__class__.__name__,
                                                          database_verbose, builder.__class__.__name__, name, builder.name,
                                                          (time.time() - start_time) * 1000))
                beautify_print("args: " + human_repr_object(args + tuple(kwargs.items())))
                beautify_print("result: " + human_repr_object(result))
                print()
            return result
        return _

    def cache_builder_warper(database, func):
        def _(*args, **kwargs):
            builder = func(*args, **kwargs)
            builder.get = cache_do_warper(database, builder, builder.get, "get")
            builder.set = cache_do_warper(database, builder, builder.set, "set")
            builder.delete = cache_do_warper(database, builder, builder.delete, "delete")
            return builder
        return _

    for name, database in list(tasker.databases.items()):
        database.query = builder_warper(database, database.query)
        database.insert = builder_warper(database, database.insert)
        database.update = builder_warper(database, database.update)
        database.delete = builder_warper(database, database.delete)
        database.cache = cache_builder_warper(database, database.cache)

def load_dependency(parent, filename, parent_arguments, ap, register_aps):
    tasker = CoreTasker(filename, parent.manager, parent)
    arguments = tasker.load()
    setattr(tasker, "parent_arguments", parent_arguments)

    for argument in arguments:
        if argument["name"] in parent_arguments:
            continue

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
    for filename in tasker.get_dependencys():
        if isinstance(filename, list) and len(filename) == 2:
            filename, dependency_arguments = filename[0], (filename[1] if isinstance(filename[1], dict) else {})
        else:
            dependency_arguments = {}
        dependency_taskers.append(load_dependency(tasker, filename, dependency_arguments, ap, register_aps))
    return (tasker, dependency_taskers)

def compile_dependency(arguments, tasker, dependency_taskers):
    kn, knl = (tasker.name + "@"), len(tasker.name + "@")
    tasker_arguments = {}
    if hasattr(tasker, "parent_arguments"):
        tasker_arguments.update(tasker.parent_arguments)
    tasker_arguments.update({key[knl:]: value for key, value in arguments.items() if key[:knl] == kn})
    tasker.compile(tasker_arguments)
    if "@verbose" in arguments and arguments["@verbose"]:
        warp_database_logging(tasker)
    for dependency_tasker in dependency_taskers:
        compile_dependency(arguments, *dependency_tasker)

def run_dependency(tasker, dependency_taskers):
    dependency_statistics = []
    for dependency_tasker in dependency_taskers:
        dependency_statistics.append(run_dependency(*dependency_tasker))
    try:
        statistics = tasker.run()
    except SystemError as e:
        tasker.close(False, "signal terminaled")
        raise
    except KeyboardInterrupt as e:
        tasker.close(False, "user terminaled")
        raise
    except Exception as e:
        tasker.close(False, "Error: " + repr(e), traceback.format_exc())
        raise
    else:
        tasker.close()
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

def run(register_aps, ap_arguments, arguments, manager, tasker, dependency_taskers):
    try:
        arguments = {key.lower(): value for key, value in os.environ.items()}
        arguments.update(ap_arguments.__dict__)

        fix_print_outputer(tasker, register_aps, arguments)
        tasker.compile(arguments)
        for dependency_tasker in dependency_taskers:
            compile_dependency(arguments, *dependency_tasker)

        if "@show" in arguments and arguments["@show"]:
            for dependency_tasker in dependency_taskers:
                show_dependency_tasker(*dependency_tasker)
            show_tasker(tasker)
            return 0
        if "@verbose" in arguments and arguments["@verbose"]:
            warp_database_logging(tasker)

        for dependency_tasker in dependency_taskers:
            run_dependency(*dependency_tasker)
        tasker.run()
    except SystemError:
        tasker.close(False, "signal terminaled")
        get_logger().error("signal exited")
        return 130
    except KeyboardInterrupt:
        tasker.close(False, "user terminaled")
        get_logger().error("Crtl+C exited")
        return 130
    except Exception as e:
        if "@show" in arguments and arguments["@show"]:
            for dependency_tasker in dependency_taskers:
                show_dependency_tasker(*dependency_tasker)
            show_tasker(tasker)
        else:
            tasker.close(False, "Error: " + repr(e), traceback.format_exc())
        get_logger().error("%s\n%s", e, traceback.format_exc())
        return 1
    else:
        if "@show" in arguments and arguments["@show"]:
            return 0
        tasker.close()
    finally:
        manager.close()
    return 0

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
        if sys.platform != "win32":
            signal.signal(signal.SIGHUP, lambda signum, frame: tasker.terminate())
            signal.signal(signal.SIGTERM, lambda signum, frame: tasker.terminate())

        manager = TaskerManager(DatabaseManager())
        tasker = CoreTasker(sys.argv[1], manager)
        arguments = tasker.load()
        tasker.config_logging()

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
        for filename in tasker.get_dependencys():
            if isinstance(filename, list) and len(filename) == 2:
                filename, dependency_arguments = filename[0], (filename[1] if isinstance(filename[1], dict) else {})
            else:
                dependency_arguments = {}
            dependency_taskers.append(load_dependency(tasker, filename, dependency_arguments, ap, register_aps))
        ap_arguments = ap.parse_args()
        exit(run(register_aps, ap_arguments, arguments, manager, tasker, dependency_taskers))
    except SystemError:
        get_logger().error("signal exited")
        exit(130)
    except KeyboardInterrupt:
        get_logger().error("Crtl+C exited")
        exit(130)
    except Exception as e:
        get_logger().error("%s\n%s", e, traceback.format_exc())
        exit(1)

if __name__ == "__main__":
    main()