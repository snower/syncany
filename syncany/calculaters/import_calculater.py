# -*- coding: utf-8 -*-
# 2020/11/3
# create by: snower

import types
import traceback
from ..logger import get_logger
from .calculater import Calculater


IMPORT_MODULES = {}


class ImportCalculater(Calculater):
    def __init__(self, *args):
        super(ImportCalculater, self).__init__(*args)

        if len(self._import_name) + 2 < len(self.name):
            self.calculate_name = self.name[(len(self._import_name) + 2):]
            attr_names = self.calculate_name.split(".")
            self.module_or_func = self._import_module
            for attr_name in attr_names:
                if not hasattr(self.module_or_func, attr_name):
                    raise NotImplementedError("%s not implemented %s" % (self._import_module, self.calculate_name))
                self.module_or_func = getattr(self.module_or_func, attr_name)
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
