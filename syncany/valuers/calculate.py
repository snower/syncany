# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from .valuer import Valuer

class CalculateValuer(Valuer):
    def __init__(self, calculater, calculater_name, args_valuers, return_valuer, inherit_valuers, *args, **kwargs):
        self.calculater = calculater
        self.calculater_name = calculater_name
        self.args_valuers = args_valuers
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(CalculateValuer, self).__init__(*args, **kwargs)

    def init_valuer(self):
        self.wait_loaded = True if not self.return_valuer else False

        if self.return_valuer:
            self.check_wait_loaded(self.args_valuers)

    def check_wait_loaded(self, valuers):
        for valuer in valuers:
            if valuer.require_loaded():
                self.wait_loaded = True
                return

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        args_valuers = []
        for valuer in self.args_valuers:
            args_valuers.append(valuer.clone())
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(self.calculater, self.calculater_name, args_valuers, return_valuer, inherit_valuers,
                              self.key, self.filter, wait_loaded=self.wait_loaded)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        for valuer in self.args_valuers:
            valuer.fill(data)

        if not self.wait_loaded:
            values = []
            for valuer in self.args_valuers:
                values.append(valuer.get())

            calculater = self.calculater(self.calculater_name, *values)
            self.do_filter(calculater.calculate())
            self.return_valuer.fill(self.value)
        return self

    def get(self):
        if self.wait_loaded:
            values = []
            for valuer in self.args_valuers:
                values.append(valuer.get())

            calculater = self.calculater(self.calculater_name, *values)
            self.do_filter(calculater.calculate())
            if self.return_valuer:
                self.return_valuer.fill(self.value)

        if self.return_valuer:
            self.value = self.return_valuer.get()
        return self.value

    def childs(self):
        childs = []
        if self.args_valuers:
            for args_valuer in self.args_valuers:
                childs.append(args_valuer)
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        fields = []
        for valuer in self.args_valuers:
            for field in valuer.get_fields():
                fields.append(field)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        if self.return_valuer:
            return self.return_valuer.get_final_filter()

        if self.filter:
            return self.filter

        final_filter = None
        for valuer in self.args_valuers:
            filter = valuer.get_final_filter()
            if filter is None:
                continue

            if final_filter is not None and final_filter.__class__ != filter.__class__:
                return None

            final_filter = filter

        return final_filter