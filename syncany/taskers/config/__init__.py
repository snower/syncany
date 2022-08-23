# -*- coding: utf-8 -*-
# 2021/10/29
# create by: snower

from .reader import ConfigReader
from .file_reader import FileConfigReader
from .http_reader import HttpConfigReader
from .parser import Parser
from .json_parser import JsonParser
from .yaml_parser import YamlParser
from ...errors import ConfigReaderUnknownException

READERS = {
    "http": HttpConfigReader,
    "https": HttpConfigReader,
    "file": FileConfigReader
}

PARSERS = {
    "json": JsonParser,
    "yaml": YamlParser
}


def load_config(filename):
    reader_type = filename.split("://")[0].lower() if "://" in filename else "file"
    if reader_type not in READERS:
        raise ConfigReaderUnknownException("%s reader is unknown" % reader_type)
    reader = READERS[reader_type](filename)
    content_type, content = reader.read()
    if content_type not in PARSERS:
        return content
    parser = PARSERS[content_type](content)
    return parser.parse()

def register_reader(name, reader):
    if not issubclass(reader, ConfigReader):
        raise TypeError("is not ConfigReader")
    READERS[name] = reader
    return reader

def register_parser(name, parser):
    if not issubclass(parser, Parser):
        raise TypeError("is not Parser")
    PARSERS[name] = parser
    return parser