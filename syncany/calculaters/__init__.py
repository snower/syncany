# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

import datetime
import uuid
from .calculater import Calculater, TypeFormatCalculater, TypingCalculater, MathematicalCalculater
from ..filters import Filter, IntFilter, FloatFilter, StringFilter, BytesFilter, BooleanFilter, ArrayFilter, SetFilter, \
    MapFilter, ObjectIdFilter, UUIDFilter, DateTimeFilter, DateFilter, TimeFilter
from .builtin import *
from .convert_calculater import *
from .datetime_calculater import *
from .transform_calculater import TransformCalculater, TransformVHKCalculater
from .textline_calculater import TextLineCalculater
from ..errors import CalculaterUnknownException
try:
    from bson.objectid import ObjectId
except ImportError:
    ObjectId = None


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
    'is_set': IsSetCalculater,
    'is_map': IsMapCalculater,
    "is_objectid": IsObjectIdCalculater,
    "is_uuid": IsUUIDCalculater,
    "is_datetime": IsDateTimeCalculater,
    "is_date": IsDateCalculater,
    "is_time": IsTimeCalculater,
    "is": IsCalculater,
    "convert_int": ConvertIntCalculater,
    "convert_float": ConvertFloatCalculater,
    "convert_string": ConvertStringCalculater,
    "convert_bytes": ConvertBytesCalculater,
    'convert_bool': ConvertBooleanCalculater,
    'convert_array': ConvertArrayCalculater,
    'convert_set': ConvertSetCalculater,
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

    "not": NotCalculater,
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

    if isinstance(CALCULATERS[name], str):
        module_name, _, cls_name = CALCULATERS[name].rpartition(".")
        if module_name[0] == ".":
            module_name = module_name[1:]
            module = __import__(module_name, globals(), locals(), [module_name], 1)
        elif "." in module_name:
            from_module_name, _, module_name = module_name.rpartition(".")
            module = __import__(from_module_name, globals(), locals(), [module_name])
        else:
            module = __import__(module_name, globals(), locals())
        calculater_cls = getattr(module, cls_name)
        if not isinstance(calculater_cls, type) or not issubclass(calculater_cls, Calculater):
            raise TypeError("is not Calculater")
        CALCULATERS[name] = calculater_cls
    elif isinstance(CALCULATERS[name], (types.FunctionType, types.LambdaType)):
        calculater_cls = CALCULATERS[name]()
        if not isinstance(calculater_cls, type) or not issubclass(calculater_cls, Calculater):
            raise TypeError("is not Calculater")
        CALCULATERS[name] = calculater_cls
    return CALCULATERS[name]

def register_calculater(name, calculater=None):
    if calculater is None:
        def _(calculater):
            if not isinstance(calculater, str) and not callable(calculater) \
                    and (not isinstance(calculater, type) or not issubclass(calculater, Calculater)):
                raise TypeError("is not Calculater")
            CALCULATERS[name] = calculater
            return calculater
        return _

    if not isinstance(calculater, str) and not callable(calculater) \
            and (not isinstance(calculater, type) or not issubclass(calculater, Calculater)):
        raise TypeError("is not Calculater")
    CALCULATERS[name] = calculater
    return calculater

def typing_filter(type_or_filter):
    def _(cls_or_func):
        def get_final_filter():
            if issubclass(type_or_filter, Filter):
                final_filter = type_or_filter.default()
            elif isinstance(type_or_filter, Filter):
                final_filter = type_or_filter
            elif type_or_filter is int or issubclass(type_or_filter, int):
                final_filter = IntFilter.default()
            elif type_or_filter is float or issubclass(type_or_filter, float):
                final_filter = FloatFilter.default()
            elif type_or_filter is str or issubclass(type_or_filter, str):
                final_filter = StringFilter.default()
            elif type_or_filter is bool or issubclass(type_or_filter, bool):
                final_filter = BooleanFilter.default()
            elif type_or_filter is datetime.datetime or issubclass(type_or_filter, datetime.datetime):
                final_filter = DateTimeFilter.default()
            elif type_or_filter is datetime.date or issubclass(type_or_filter, datetime.date):
                final_filter = DateFilter.default()
            elif type_or_filter is datetime.time or issubclass(type_or_filter, datetime.time):
                final_filter = TimeFilter.default()
            elif type_or_filter is bytes or issubclass(type_or_filter, bytes):
                final_filter = BytesFilter.default()
            elif type_or_filter is list or issubclass(type_or_filter, list):
                final_filter = ArrayFilter.default()
            elif type_or_filter is set or issubclass(type_or_filter, set):
                final_filter = SetFilter.default()
            elif type_or_filter is dict or issubclass(type_or_filter, dict):
                final_filter = MapFilter.default()
            elif ObjectId is not None and type_or_filter is ObjectId or issubclass(type_or_filter, ObjectId):
                final_filter = ObjectIdFilter.default()
            elif type_or_filter is uuid.UUID or issubclass(type_or_filter, uuid.UUID):
                final_filter = UUIDFilter.default()
            else:
                final_filter = None
            setattr(cls_or_func, "get_final_filter", lambda: final_filter)
            return final_filter
        setattr(cls_or_func, "get_final_filter", get_final_filter)
        return cls_or_func
    return _
