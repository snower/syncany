# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from .valuer import Valuer

class CalculateValuer(Valuer):
    def __init__(self, calculater, args_valuers, *args, **kwargs):
        super(CalculateValuer, self).__init__(*args, **kwargs)

        self.calculater = calculater
        self.args_valuers = args_valuers

    def clone(self):
        args_valuers = []
        for valuer in self.args_valuers:
            args_valuers.append(valuer.clone())
        return self.__class__(self.calculater, args_valuers, self.key, self.filter)

    def fill(self, data):
        super(CalculateValuer, self).fill(data)

        for valuer in self.args_valuers:
            valuer.fill(data)
        return self

    def get(self):
        values = []
        for valuer in self.args_valuers:
            values.append(valuer.get())

        calculater = self.calculater(*values)
        self.value = calculater.calculate()
        if self.filter:
            self.value = self.filter.filter(self.value)
        return self.value

    def childs(self):
        return self.args_valuers

    def get_fields(self):
        fields = []
        for valuer in self.args_valuers:
            for field in valuer.get_fields():
                fields.append(field)

        return fields