# -*- coding: utf-8 -*-
# 2020/11/3
# create by: snower

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

        if len(self.args) == 1 and isinstance(self.args[0], list):
            return module_or_func(*tuple(self.args[0]))
        return module_or_func(*tuple(self.args))


def create_import_calculater(name, module):
    name = "".join([n[:1].upper() + n[1:] for n in name.split("_")])
    return type(name + "ImportCalculater", (ImportCalculater,), dict(_import_name=name, _import_module=module))