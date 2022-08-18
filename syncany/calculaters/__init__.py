# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from .calculater import Calculater
from .builtin import *
from .conversion_calculater import ConvCalculater
from ..errors import CalculaterUnknownException

CALCULATERS = {
    "": Calculater,
    "type": TypeCalculater,
    'range': RangeCalculater,
    "add": AddCalculater,
    "sub": SubCalculater,
    "mul": MulCalculater,
    "div": DivCalculater,
    "mod": ModCalculater,
    "bit": BitCalculater,
    "substring": SubstringCalculater,
    "split": SplitCalculater,
    "join": JoinCalculater,
    "now": NowCalculater,
    "empty": EmptyCalculater,
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
    "sort": SortCalculater,
    "string": StringCalculater,
    "array": ArrayCalculater,
    "map": MapCalculater,
    "math": MathCalculater,
    "hash": HashCalculater,
    "json": JsonCalculater,
    "struct": StructCalculater,
    "conv": ConvCalculater,
}

def find_calculater(name):
    name = name.split("::")[0]
    if name not in CALCULATERS:
        raise CalculaterUnknownException("%s is unknown calculater" % name)
    return CALCULATERS[name]

def register_calculater(name, calculater):
    if not issubclass(calculater, Calculater):
        raise TypeError("is not Calculater")
    CALCULATERS[name] = calculater
    return calculater