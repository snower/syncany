# -*- coding: utf-8 -*-
# 2020/11/3
# create by: snower

import types
import traceback
from ..logger import get_logger
from .calculater import Calculater


class ImportCalculater(Calculater):
    def calculate(self, *args):
        if len(self._import_name) + 2 < len(self.name):
            calculate_name = self.name[(len(self._import_name) + 2):]
            attr_names = calculate_name.split(".")
            module_or_func = self._import_module
            for attr_name in attr_names:
                if not hasattr(module_or_func, attr_name):
                    raise NotImplementedError("%s not implemented %s" % (self._import_module, calculate_name))
                module_or_func = getattr(module_or_func, attr_name)
            if not callable(module_or_func):
                raise NotImplementedError("%s not callable %s" % (self._import_module, calculate_name))
        else:
            calculate_name = self.name
            if not callable(self._import_module):
                raise NotImplementedError("%s not callable %s" % (self._import_module, self.name))
            module_or_func = self._import_module

        try:
            if not args:
                return module_or_func()
            if len(args) == 1 and isinstance(args[0], list) and args[0] and isinstance(args[0][0], dict):
                try:
                    return module_or_func(*tuple(args[0]))
                except TypeError:
                    pass
            return module_or_func(*args)
        except Exception as e:
            get_logger().warning("import calculater execute %s(%s) error: %s\n%s", calculate_name, args, e,
                                 traceback.format_exc())
            return None


def create_import_calculater(name, module_or_func):
    class_name = "".join([n[:1].upper() + n[1:] for n in name.split("_")]) + "ImportCalculater"
    if isinstance(module_or_func, (types.FunctionType, types.BuiltinFunctionType, types.LambdaType)):
        import_module = lambda self, *args, **kwargs: module_or_func(*args, **kwargs)
    else:
        import_module = module_or_func
    return type(class_name, (ImportCalculater,), dict(_import_name=name, _import_module=import_module))
