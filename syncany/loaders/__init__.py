# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .loader import Loader
from .const import ConstLoader
from .db import DBLoader
from .db_join import DBJoinLoader
from .db_pull import DBPullLoader
from ..errors import LoaderUnknownException

LOADERS = {
    "const_loader": ConstLoader,
    "db_loader": DBLoader,
    "db_join_loader": DBJoinLoader,
    "db_pull_loader": DBPullLoader,
}

def find_loader(name):
    if name not in LOADERS:
        raise LoaderUnknownException("%s is unknown loader" % name)
    return LOADERS[name]

def register_loader(name, loader):
    if not issubclass(loader, Loader):
        raise TypeError("is not Loader")
    LOADERS[name] = loader
    return loader