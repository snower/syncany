# -*- coding: utf-8 -*-
# 2020/11/3
# create by: snower

import datetime
import uuid
import types
import traceback
from ..logger import get_logger
from .calculater import Calculater
from ..filters import IntFilter, FloatFilter, StringFilter, BytesFilter, BooleanFilter, ArrayFilter, SetFilter, \
    MapFilter, ObjectIdFilter, UUIDFilter, DateTimeFilter, DateFilter, TimeFilter
try:
    from bson.objectid import ObjectId
except ImportError:
    ObjectId = None


IMPORT_MODULES = {}


def parse_final_filter(module_or_func):
    if not callable(module_or_func):
        return None
    return_type = None
    if isinstance(module_or_func, (types.FunctionType, types.BuiltinFunctionType, types.LambdaType)):
        if hasattr(module_or_func, "__annotations__"):
            return_type = module_or_func.__annotations__.get("return")
    else:
        if hasattr(module_or_func, "__call__") and hasattr(module_or_func.__call__, "__annotations__"):
            return_type = module_or_func.__annotations__.get("return")
    if return_type is None:
        return None
    if return_type is int or issubclass(return_type, int):
        final_filter = IntFilter.default()
    elif return_type is float or issubclass(return_type, float):
        final_filter = FloatFilter.default()
    elif return_type is str or issubclass(return_type, str):
        final_filter = StringFilter.default()
    elif return_type is bool or issubclass(return_type, bool):
        final_filter = BooleanFilter.default()
    elif return_type is datetime.datetime or issubclass(return_type, datetime.datetime):
        final_filter = DateTimeFilter.default()
    elif return_type is datetime.date or issubclass(return_type, datetime.date):
        final_filter = DateFilter.default()
    elif return_type is datetime.time or issubclass(return_type, datetime.time):
        final_filter = TimeFilter.default()
    elif return_type is bytes or issubclass(return_type, bytes):
        final_filter = BytesFilter.default()
    elif return_type is list or issubclass(return_type, list):
        final_filter = ArrayFilter.default()
    elif return_type is set or issubclass(return_type, set):
        final_filter = SetFilter.default()
    elif return_type is dict or issubclass(return_type, dict):
        final_filter = MapFilter.default()
    elif ObjectId is not None and return_type is ObjectId or issubclass(return_type, ObjectId):
        final_filter = ObjectIdFilter.default()
    elif return_type is uuid.UUID or issubclass(return_type, uuid.UUID):
        final_filter = UUIDFilter.default()
    else:
        final_filter = None
    setattr(module_or_func, "get_final_filter", lambda: final_filter)
    return final_filter


class ImportCalculater(Calculater):
    def __init__(self, *args):
        super(ImportCalculater, self).__init__(*args)

        if len(self._import_name) + 2 < len(self.name):
            self.calculate_name = self.name[(len(self._import_name) + 2):]
            attr_names = self.calculate_name.split(".")
            self.module_or_func = self._import_module
            for attr_name in attr_names:
                if hasattr(self.module_or_func, attr_name):
                    self.module_or_func = getattr(self.module_or_func, attr_name)
                    continue
                try:
                    lower_attr_name = attr_name.lower()
                    lower_camel_attr_name = "".join([n[:1].upper() + n[1:] for n in attr_name.split("_")]).lower()
                    has_module_attr_value = False
                    for module_attr_name, module_attr_value in self.module_or_func.__dict__.items():
                        lower_module_attr_name = module_attr_name.lower()
                        if lower_attr_name == lower_module_attr_name or lower_camel_attr_name == lower_module_attr_name:
                            self.module_or_func = module_attr_value
                            has_module_attr_value = True
                            break
                    if has_module_attr_value:
                        continue
                except AttributeError:
                    pass
                raise NotImplementedError("%s not implemented %s" % (self._import_module, self.calculate_name))
        else:
            self.calculate_name = self.name
            if not callable(self._import_module):
                raise NotImplementedError("%s not callable %s" % (self._import_module, self.name))
            self.module_or_func = self._import_module

    def calculate(self, *args):
        try:
            if not args:
                if not callable(self.module_or_func):
                    return self.module_or_func
                return self.module_or_func()

            if not callable(self.module_or_func):
                raise NotImplementedError("%s not callable %s" % (self._import_module, self.calculate_name))
            if len(args) == 1 and isinstance(args[0], list) and args[0] and isinstance(args[0][0], dict):
                try:
                    return self.module_or_func(*tuple(args[0]))
                except TypeError:
                    pass
            return self.module_or_func(*args)
        except Exception as e:
            get_logger().warning("import calculater execute %s(%s) error: %s\n%s", self.calculate_name, args, e,
                                 traceback.format_exc())
            return None

    def get_final_filter(self):
        if hasattr(self.module_or_func, "get_final_filter"):
            return self.module_or_func.get_final_filter()
        return parse_final_filter(self.module_or_func)


def create_import_calculater(name, module_or_func):
    module_id = (name, id(module_or_func))
    if module_id in IMPORT_MODULES:
        return IMPORT_MODULES[module_id]
    class_name = "".join([n[:1].upper() + n[1:] for n in name.split("_")]) + "ImportCalculater"
    if isinstance(module_or_func, (types.FunctionType, types.BuiltinFunctionType, types.LambdaType)):
        import_module = lambda self, *args, **kwargs: module_or_func(*args, **kwargs)
    else:
        import_module = module_or_func
    IMPORT_MODULES[module_id] = type(class_name, (ImportCalculater,), dict(_import_name=name, _import_module=import_module))
    return IMPORT_MODULES[module_id]
