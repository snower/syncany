# -*- coding: utf-8 -*-
# 2020/7/2
# create by: snower

import types
from .valuer import Valuer, LoadAllFieldsException

class YieldValuer(Valuer):
    def __init__(self, value_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.value_valuer = value_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(YieldValuer, self).__init__(*args, **kwargs)

        self.iter_valuers = []
        self.iter_datas = []

    def init_valuer(self):
        self.wait_loaded = True if not self.return_valuer else False
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
        return self.__class__(value_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, wait_loaded=self.wait_loaded)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.value_valuer:
            self.value_valuer.fill(data)

        if self.return_valuer and not self.wait_loaded:
            if self.value_valuer:
                data = self.value_valuer.get()

            if isinstance(data, list):
                for d in data:
                    return_valuer = self.return_valuer.clone()
                    return_valuer.fill(self.do_filter(d))
                    self.iter_valuers.append(return_valuer)
            else:
                return_valuer = self.return_valuer.clone()
                return_valuer.fill(self.do_filter(data))
                self.iter_valuers.append(return_valuer)
            return self

        if not self.value_valuer:
            if isinstance(data, list):
                self.iter_datas = [self.do_filter(d) for d in data]
            else:
                self.iter_datas = [self.do_filter(data)]
        return self

    def get(self):
        if self.return_valuer:
            if self.wait_loaded:
                if self.value_valuer:
                    data = self.value_valuer.get()
                else:
                    data = self.iter_datas

                if isinstance(data, list):
                    for d in data:
                        return_valuer = self.return_valuer.clone()
                        return_valuer.fill(self.do_filter(d))
                        self.iter_valuers.append(return_valuer)
                else:
                    return_valuer = self.return_valuer.clone()
                    return_valuer.fill(self.do_filter(data))
                    self.iter_valuers.append(return_valuer)

            self.iter_datas = []
            for valuer in self.iter_valuers:
                self.iter_datas.append(valuer.get())
        else:
            if self.value_valuer:
                data = self.value_valuer.get()
                if isinstance(data, list):
                    self.iter_datas = [self.do_filter(d) for d in data]
                else:
                    self.iter_datas = [self.do_filter(data)]

        def gen_iter():
            gdata = yield None
            for data in self.iter_datas:
                if isinstance(data, types.GeneratorType):
                    while True:
                        try:
                            child_data = data.send(gdata)
                            gdata = yield child_data
                        except StopIteration:
                            break
                else:
                    gdata = yield data
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
        is_pass, fields = False, []
        try:
            if self.value_valuer:
                for field in self.value_valuer.get_fields():
                    fields.append(field)
        except LoadAllFieldsException:
            is_pass = True

        if (not self.value_valuer or is_pass) and self.return_valuer:
            for field in self.return_valuer.get_fields():
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
        return None