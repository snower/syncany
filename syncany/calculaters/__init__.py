# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from .builtin import *

CALCULATERS = {
    "add": AddCalculater,
    "sub": SubCalculater,
    "mul": MulCalculater,
    "div": DivCalculater,
    "concat": ConcatCalculater,
    "substring": SubstringCalculater,
    "split": SplitCalculater,
    "join": JoinCalculater,
}

def find_calculater(name):
    return CALCULATERS.get(name)