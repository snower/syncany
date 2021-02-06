# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .filter import Filter
from .builtin import *
from ..errors import FilterUnknownException

FILTERS = {
    "int": IntFilter,
    "float": FloatFilter,
    "str": StringFilter,
    "bytes": BytesFilter,
    'bool': BooleanFilter,
    'array': ArrayFilter,
    'map': MapFilter,
    "ObjectId": ObjectIdFilter,
    "datetime": DateTimeFilter,
    "date": DateFilter,
    "time": TimeFilter,
}

def find_filter(name):
    if name not in FILTERS:
        return None
    return FILTERS[name]

def register_filter(name, filter):
    if not issubclass(filter, Filter):
        raise TypeError("is not Filter")
    FILTERS[name] = filter
    return filter