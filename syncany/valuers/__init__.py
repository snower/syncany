# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .const import ConstValuer
from .const_join import ConstJoinValuer
from .db import DBValuer
from .db_join import DBJoinValuer
from .case import CaseValuer

VALUERS = {
    "const_valuer": ConstValuer,
    "const_join_valuer": ConstJoinValuer,
    "db_valuer": DBValuer,
    "db_join_valuer": DBJoinValuer,
    "case_valuer": CaseValuer,
}

def find_valuer(name):
    return VALUERS.get(name)