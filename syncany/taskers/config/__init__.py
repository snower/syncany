# -*- coding: utf-8 -*-
# 2021/10/29
# create by: snower

import os
from .json_parser import JsonParser
from .yaml_parser import YamlParser

def load_file(filename):
    with open(filename, "r", encoding=os.environ.get("SYNCANYENCODING", "utf-8")) as fp:
        content = fp.read()
        filename_infos = filename.split(".")
        if not filename_infos:
            return content
        if filename_infos[-1] == "json":
            parser = JsonParser(content)
            return parser.parse()
        if filename_infos[-1] == "yaml":
            parser = YamlParser(content)
            return parser.parse()
        return content

def load_http(url):
    try:
        import requests
    except:
        raise ImportError("requests>=2.22.0 is required")
    res = requests.get(url)
    url_infos = url.split(".")
    content_type = res.headers.get("Content-Type") or (url_infos[-1] if url_infos else "")
    if "json" in content_type:
        parser = JsonParser(res.text)
        return parser.parse()
    if "yaml" in content_type:
        parser = YamlParser(res.text)
        return parser.parse()
    return res.text

def load_config(filename):
    if filename[:5].lower() == "http:" or filename[:6].lower() == "https:":
        return load_http(filename)
    return load_file(filename)