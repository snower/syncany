# -*- coding: utf-8 -*-
# 2020/11/3
# create by: snower

from .calculater import Calculater


class ImportCalculater(Calculater):
    def calculate(self):
        calculate_name = self.name[(len(self._import_name) + 2):]
        if not hasattr(self._import_module, calculate_name):
            raise NotImplementedError("%s not implemented %s" % (self._import_module, calculate_name))

        if len(self.args) == 1 and isinstance(self.args[0], list):
            return getattr(self._import_module, calculate_name)(*tuple(self.args[0]))
        return getattr(self._import_module, calculate_name)(*tuple(self.args))


def create_import_calculater(name, module):
    name = "".join([n[:1].upper() + n[1:] for n in name.split("_")])
    return type(name + "ImportCalculater", (ImportCalculater,), dict(_import_name=name, _import_module=module))