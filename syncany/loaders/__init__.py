# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .const import ConstLoader
from .db import DBLoader
from .db_join import DBJoinLoader

LOADERS = {
    "const_loader": ConstLoader,
    "db_loader": DBLoader,
    "db_join_loader": DBJoinLoader,
}

def find_loader(name):
    return LOADERS.get(name)