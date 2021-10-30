# -*- coding: utf-8 -*-
# 2021/10/29
# create by: snower

from .json_parser import JsonParser
from .yaml_parser import YamlParser

def load_file(filename):
    filename_infos = filename.split(".")
    if not filename_infos:
        with open(filename, "r") as fp:
            return fp.read()
    if filename_infos[-1] == "json":
        parser = JsonParser(filename)
        return parser.load()
    if filename_infos[-1] == "yaml":
        parser = YamlParser(filename)
        return parser.load()
    with open(filename, "r") as fp:
        return fp.read()