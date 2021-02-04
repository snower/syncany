# -*- coding: utf-8 -*-
# 2020/7/2
# create by: snower

import types
from .valuer import Valuer

class YieldValuer(Valuer):
    def __init__(self, value_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        super(YieldValuer, self).__init__(*args, **kwargs)

        self.value_valuer = value_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.wait_loaded = True if not self.return_valuer else False
        self.iter_valuers = []
        self.iter_datas = []

        if self.return_valuer:
            self.check_wait_loaded()

    def check_wait_loaded(self):
        if self.value_valuer and self.value_valuer.require_loaded():
            self.wait_loaded = True

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        value_valuer = self.value_valuer.clone() if self.value_valuer else None
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(value_valuer, return_valuer, inherit_valuers, self.key, self.filter)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.value_valuer:
            self.value_valuer.fill(data)

        if self.return_valuer and not self.wait_loaded:
            if self.value_valuer:
                data = self.value_valuer.get()

            if isinstance(data, (list, tuple, set)):
                for d in data:
                    return_valuer = self.return_valuer.clone()
                    return_valuer.fill(d)
                    self.iter_valuers.append(return_valuer)
            else:
                return_valuer = self.return_valuer.clone()
                return_valuer.fill(data)
                self.iter_valuers.append(return_valuer)
            return self

        if not self.value_valuer:
            self.iter_datas = data if isinstance(data, (list, tuple, set)) else [data]
        return self

    def filter_data(self, data):
        if self.filter:
            if isinstance(data, (list, tuple, set)):
                values = []
                for value in data:
                    values.append(self.filter.filter(value))
                return values
            else:
                return self.filter.filter(self.value)
        else:
            return data

    def get(self):
        if self.return_valuer:
            if self.wait_loaded:
                if self.value_valuer:
                    data = self.value_valuer.get()
                else:
                    data = self.iter_datas

                if isinstance(data, (list, tuple, set)):
                    for d in data:
                        return_valuer = self.return_valuer.clone()
                        return_valuer.fill(d)
                        self.iter_valuers.append(return_valuer)
                else:
                    return_valuer = self.return_valuer.clone()
                    return_valuer.fill(data)
                    self.iter_valuers.append(return_valuer)

            self.iter_datas = []
            for valuer in self.iter_valuers:
                self.iter_datas.append(valuer.get())
        else:
            if self.value_valuer:
                data = self.value_valuer.get()
                self.iter_datas = data if isinstance(data, (list, tuple, set)) else [data]

        def gen_iter():
            gdata = yield None
            for data in self.iter_datas:
                if isinstance(data, types.GeneratorType):
                    while True:
                        try:
                            child_data = data.send(gdata)
                            child_data = self.filter_data(child_data)
                            yield child_data
                        except StopIteration:
                            break
                else:
                    data = self.filter_data(data)
                    yield data
        g = gen_iter()
        g.send(None)
        return g

    def childs(self):
        childs = []
        if self.value_valuer:
            childs.append(self.value_valuer)
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        fields = []
        if self.value_valuer:
            for field in self.value_valuer.get_fields():
                fields.append(field)

        if not self.wait_loaded and self.return_valuer:
            for field in self.return_valuer.get_fields():
                fields.append(field)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        if self.filter:
            return self.filter

        if self.return_valuer:
            return self.return_valuer.get_final_filter()
        return None