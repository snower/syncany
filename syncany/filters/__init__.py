# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .builtin import *

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
    "datetimef": DateTimeFormatFilter,
    "date": DateFilter,
    "datef": DateFormatFilter,
}

def find_filter(name):
    return FILTERS.get(name)