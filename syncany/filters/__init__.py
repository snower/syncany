# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .filter import Filter
from .builtin import *
from ..errors import FilterUnknownException

FILTERS = {
    "int": IntFilter,
    "float": FloatFilter,
    "decimal": DecimalFilter,
    "str": StringFilter,
    "bytes": BytesFilter,
    'bool': BooleanFilter,
    'array': ArrayFilter,
    'set': SetFilter,
    'map': MapFilter,
    "objectid": ObjectIdFilter,
    "uuid": UUIDFilter,
    "datetime": DateTimeFilter,
    "date": DateFilter,
    "time": TimeFilter,
}

def find_filter(name):
    if name not in FILTERS:
        return None

    if isinstance(FILTERS[name], str):
        module_name, _, cls_name = FILTERS[name].rpartition(".")
        if module_name[0] == ".":
            module_name = module_name[1:]
            module = __import__(module_name, globals(), locals(), [module_name], 1)
        elif "." in module_name:
            from_module_name, _, module_name = module_name.rpartition(".")
            module = __import__(from_module_name, globals(), locals(), [module_name])
        else:
            module = __import__(module_name, globals(), locals())
        filter_cls = getattr(module, cls_name)
        if not isinstance(filter_cls, type) or not issubclass(filter_cls, Filter):
            raise TypeError("is not Filter")
        FILTERS[name] = filter_cls
    elif isinstance(FILTERS[name], (types.FunctionType, types.LambdaType)):
        filter_cls = FILTERS[name]()
        if not isinstance(filter_cls, type) or not issubclass(filter_cls, Filter):
            raise TypeError("is not Filter")
        FILTERS[name] = filter_cls
    return FILTERS[name]

def register_filter(name, filter=None):
    if filter is None:
        def _(filter):
            if not isinstance(filter, str) and not callable(filter) \
                    and (not isinstance(filter, type) or not issubclass(filter, Filter)):
                raise TypeError("is not Filter")
            FILTERS[name] = filter
            return filter
        return _

    if not isinstance(filter, str) and not callable(filter) \
            and (not isinstance(filter, type) or not issubclass(filter, Filter)):
        raise TypeError("is not Filter")
    FILTERS[name] = filter
    return filter