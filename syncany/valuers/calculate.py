# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from .valuer import Valuer

class CalculateValuer(Valuer):
    def __init__(self, calculater, args_valuers, return_valuer, *args, **kwargs):
        super(CalculateValuer, self).__init__(*args, **kwargs)

        self.calculater = calculater
        self.args_valuers = args_valuers
        self.return_valuer = return_valuer
        self.wait_loaded = True if not self.return_valuer else False

        if self.return_valuer:
            self.check_wait_loaded(self.args_valuers)

    def check_wait_loaded(self, valuers):
        for valuer in valuers:
            if valuer.require_loaded():
                self.wait_loaded = True
                return

            self.check_wait_loaded(valuer.childs())

    def clone(self):
        args_valuers = []
        for valuer in self.args_valuers:
            args_valuers.append(valuer.clone())
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        return self.__class__(self.calculater, args_valuers, return_valuer, self.key, self.filter)

    def fill(self, data):
        super(CalculateValuer, self).fill(data)

        for valuer in self.args_valuers:
            valuer.fill(data)

        if not self.wait_loaded:
            values = []
            for valuer in self.args_valuers:
                values.append(valuer.get())

            calculater = self.calculater(*values)
            self.return_valuer.fill(calculater.calculate())
        return self

    def get(self):
        if not self.wait_loaded:
            self.value = self.return_valuer.get()
        else:
            values = []
            for valuer in self.args_valuers:
                values.append(valuer.get())

            calculater = self.calculater(*values)
            if self.return_valuer:
                self.return_valuer.fill(calculater.calculate())
                self.value = self.return_valuer.get()
            else:
                self.value = calculater.calculate()

        if self.filter:
            if isinstance(self.value, (list, tuple, set)):
                values = []
                for value in self.value:
                    values.append(self.filter.filter(value))
                self.value = values
            else:
                self.value = self.filter.filter(self.value)
        return self.value

    def childs(self):
        if not self.return_valuer:
            return self.args_valuers
        return self.args_valuers + [self.return_valuer]

    def get_fields(self):
        fields = []
        for valuer in self.args_valuers:
            for field in valuer.get_fields():
                fields.append(field)

        if self.return_valuer:
            for field in self.return_valuer.get_fields():
                fields.append(field)
        return fields

    def get_final_filter(self):
        if self.filter:
            return self.filter

        if self.return_valuer:
            return self.return_valuer.get_final_filter()

        final_filter = None
        for valuer in self.childs():
            filter = valuer.get_final_filter()
            if filter is None:
                continue

            if final_filter is not None and final_filter.__class__ != filter.__class__:
                return None

            final_filter = filter

        return final_filter