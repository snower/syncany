# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from .calculater import Calculater
from .builtin import *

CALCULATERS = {
    "": Calculater,
    "type": TypeCalculater,
    "add": AddCalculater,
    "sub": SubCalculater,
    "mul": MulCalculater,
    "div": DivCalculater,
    "mod": ModCalculater,
    "bit": BitCalculater,
    "concat": ConcatCalculater,
    "substring": SubstringCalculater,
    "split": SplitCalculater,
    "join": JoinCalculater,
    "now": NowCalculater,
    "gt": GtCalculater,
    "gte": GteCalculater,
    "lt": LtCalculater,
    "lte": LteCalculater,
    "eq": EqCalculater,
    "neq": NeqCalculater,
    "and": AndCalculater,
    "or": OrCalculater,
    "in": InCalculater,
    "max": MaxCalculater,
    "min": MinCalculater,
    "len": LenCalculater,
    "abs": AbsCalculater,
    "index": IndexCalculater,
    "filter": FilterCalculater,
    "sum": SumCalculater,
    "string": StringCalculater,
    "array": ArrayCalculater,
    "map": MapCalculater,
    "math": MathCalculater,
    "hash": HashCalculater,
    "json": JsonCalculater,
}

def find_calculater(name):
    return CALCULATERS.get(name.split("::")[0])

def register_calculater(name, calculater):
    if not issubclass(calculater, Calculater):
        raise TypeError("is not Calculater")
    CALCULATERS[name] = calculater
    return calculater