# -*- coding: utf-8 -*-
# 2020/11/3
# create by: snower

import traceback
from ..logger import get_logger
from .calculater import Calculater


class ImportCalculater(Calculater):
    def calculate(self):
        calculate_name = self.name[(len(self._import_name) + 2):]
        attr_names = calculate_name.split(".")
        module_or_func = self._import_module
        for attr_name in attr_names:
            if not hasattr(module_or_func, attr_name):
                raise NotImplementedError("%s not implemented %s" % (self._import_module, calculate_name))
            module_or_func = getattr(module_or_func, attr_name)
        if not callable(module_or_func):
            raise NotImplementedError("%s not callable %s" % (self._import_module, calculate_name))

        try:
            if len(self.args) == 1 and isinstance(self.args[0], list) and self.args[0] and isinstance(self.args[0][0], dict):
                try:
                    return module_or_func(*tuple(self.args[0]))
                except TypeError:
                    pass
            return module_or_func(*tuple(self.args))
        except Exception as e:
            get_logger().warning("import calculater execute %s(%s) error: %s\n%s", calculate_name, self.args, e,
                                 traceback.format_exc())
            return None


def create_import_calculater(name, module):
    name = "".join([n[:1].upper() + n[1:] for n in name.split("_")])
    return type(name + "ImportCalculater", (ImportCalculater,), dict(_import_name=name, _import_module=module))