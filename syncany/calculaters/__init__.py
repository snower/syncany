# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from .calculater import Calculater
from .builtin import *
from .convert_calculater import *
from .datetime_calculater import *
from .transform_calculater import TransformCalculater
from .textline_calculater import TextLineCalculater
from ..errors import CalculaterUnknownException

CALCULATERS = {
    "": Calculater,
    "type": TypeCalculater,
    "convert_int": ConvertIntCalculater,
    "convert_float": ConvertFloatCalculater,
    "convert_string": ConvertStringCalculater,
    "convert_bytes": ConvertBytesCalculater,
    'convert_bool': ConvertBooleanCalculater,
    'convert_array': ConvertArrayCalculater,
    'convert_map': ConvertMapCalculater,
    "convert_objectid": ConvertObjectIdCalculater,
    "convert_uuid": ConvertUUIDCalculater,
    "convert_datetime": ConvertDateTimeCalculater,
    "convert_date": ConvertDateCalculater,
    "convert_time": ConvertTimeCalculater,

    'range': RangeCalculater,
    "add": AddCalculater,
    "sub": SubCalculater,
    "mul": MulCalculater,
    "div": DivCalculater,
    "mod": ModCalculater,
    "bit": BitCalculater,
    "neg": NegCalculater,

    "substring": SubstringCalculater,
    "split": SplitCalculater,
    "join": JoinCalculater,
    "now": NowCalculater,
    "time_window": TimeWindowCalculater,
    "empty": EmptyCalculater,
    "is_null": IsNullCalculater,
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
    "datetime": DateTimeCalculater,
    "array": ArrayCalculater,
    "map": MapCalculater,
    "math": MathCalculater,
    "hash": HashCalculater,
    "json": JsonCalculater,
    "struct": StructCalculater,
    "re": ReCalculater,
    "transform": TransformCalculater,
    "textline": TextLineCalculater
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