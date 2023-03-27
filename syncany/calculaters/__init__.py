# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from .calculater import Calculater, TypeFormatCalculater, TypingCalculater, MathematicalCalculater
from .builtin import *
from .convert_calculater import *
from .datetime_calculater import *
from .transform_calculater import TransformCalculater, TransformVHKCalculater
from .textline_calculater import TextLineCalculater
from ..errors import CalculaterUnknownException

CALCULATERS = {
    "": Calculater,
    "type": TypeCalculater,
    "make": MakeCalculater,
    "is_null": IsNullCalculater,
    "is_int": IsIntCalculater,
    "is_float": IsFloatCalculater,
    "is_number": IsNumberCalculater,
    "is_string": IsStringCalculater,
    "is_bytes": IsBytesCalculater,
    'is_bool': IsBooleanCalculater,
    'is_array': IsArrayCalculater,
    'is_map': IsMapCalculater,
    "is_objectid": IsObjectIdCalculater,
    "is_uuid": IsUUIDCalculater,
    "is_datetime": IsDateTimeCalculater,
    "is_date": IsDateCalculater,
    "is_time": IsTimeCalculater,
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

    "add": AddCalculater,
    "sub": SubCalculater,
    "mul": MulCalculater,
    "div": DivCalculater,
    "mod": ModCalculater,
    "bit": BitCalculater,
    "neg": NegCalculater,

    "gt": GtCalculater,
    "gte": GteCalculater,
    "lt": LtCalculater,
    "lte": LteCalculater,
    "eq": EqCalculater,
    "neq": NeqCalculater,
    "and": AndCalculater,
    "or": OrCalculater,
    "in": InCalculater,

    'range': RangeCalculater,
    "substring": SubstringCalculater,
    "split": SplitCalculater,
    "join": JoinCalculater,
    "now": NowCalculater,
    "time_window": TimeWindowCalculater,
    "empty": EmptyCalculater,
    "contain": ContainCalculater,

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
    "transform": TransformVHKCalculater,
    "textline": TextLineCalculater
}

def find_calculater(name):
    name = name.split("::")[0]
    if name not in CALCULATERS:
        raise CalculaterUnknownException("%s is unknown calculater" % name)
    return CALCULATERS[name]

def register_calculater(name, calculater=None):
    if calculater is None:
        def _(calculater):
            if not issubclass(calculater, Calculater):
                raise TypeError("is not Calculater")
            CALCULATERS[name] = calculater
            return calculater
        return _

    if not issubclass(calculater, Calculater):
        raise TypeError("is not Calculater")
    CALCULATERS[name] = calculater
    return calculater