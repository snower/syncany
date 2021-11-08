# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .valuer import Valuer
from .const import ConstValuer
from .data import DataValuer
from .inherit import InheritValuer
from .db_join import DBJoinValuer
from .case import CaseValuer
from .calculate import CalculateValuer
from .schema import SchemaValuer
from .make import MakeValuer
from .let import LetValuer
from .generator import YieldValuer
from .aggregate import AggregateValuer
from .call import CallValuer
from .assign import AssignValuer
from .function import LambdaValuer
from .loop import ForeachValuer, BreakValuer, ContinueValuer
from .condition import IfValuer
from .match import MatchValuer
from .state import StateValuer
from .cache import CacheValuer
from ..errors import ValuerUnknownException

VALUERS = {
    "const_valuer": ConstValuer,
    "data_valuer": DataValuer,
    "inherit_valuer": InheritValuer,
    "db_join_valuer": DBJoinValuer,
    "case_valuer": CaseValuer,
    "calculate_valuer": CalculateValuer,
    "schema_valuer": SchemaValuer,
    "make_valuer": MakeValuer,
    "let_valuer": LetValuer,
    "yield_valuer": YieldValuer,
    "aggregate_valuer": AggregateValuer,
    "call_valuer": CallValuer,
    "assign_valuer": AssignValuer,
    "lambda_valuer": LambdaValuer,
    "foreach_valuer": ForeachValuer,
    "break_valuer": BreakValuer,
    "continue_valuer": ContinueValuer,
    "if_valuer": IfValuer,
    "match_valuer": MatchValuer,
    "state_valuer": StateValuer,
    "cache_valuer": CacheValuer,
}

def find_valuer(name):
    if name not in VALUERS:
        raise ValuerUnknownException("%s is unknown valuer" % name)
    return VALUERS[name]

def register_valuer(name, valuer):
    if not issubclass(valuer, Valuer):
        raise TypeError("is not Valuer")
    VALUERS[name] = valuer
    return valuer