# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .builtin import *

FILTERS = {
    "int": IntFilter,
    "float": FloatFilter,
    "str": StringFilter,
    "ObjectId": ObjectIdFilter,
    "datetime": DateTimeFilter,
    "datetimef": DateTimeFormatFilter,
    "date": DateFilter,
    "datef": DateFormatFilter,
}

def find_filter(name):
    return FILTERS.get(name)