# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from .calculater import Calculater
from .builtin import *

CALCULATERS = {
    "": Calculater,
    "add": AddCalculater,
    "sub": SubCalculater,
    "mul": MulCalculater,
    "div": DivCalculater,
    "concat": ConcatCalculater,
    "substring": SubstringCalculater,
    "split": SplitCalculater,
    "join": JoinCalculater,
    "now": NowCalculater,
}

def find_calculater(name):
    return CALCULATERS.get(name)