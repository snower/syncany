# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .const import ConstValuer
from .const_join import ConstJoinValuer
from .db import DBValuer
from .inherit import InheritValuer
from .db_join import DBJoinValuer
from .case import CaseValuer
from .calculate import CalculateValuer
from .schema import SchemaValuer
from .make import MakeValuer

VALUERS = {
    "const_valuer": ConstValuer,
    "const_join_valuer": ConstJoinValuer,
    "db_valuer": DBValuer,
    "inherit_valuer": InheritValuer,
    "db_join_valuer": DBJoinValuer,
    "case_valuer": CaseValuer,
    "calculate_valuer": CalculateValuer,
    "schema_valuer": SchemaValuer,
    "make_valuer": MakeValuer,
}

def find_valuer(name):
    return VALUERS.get(name)