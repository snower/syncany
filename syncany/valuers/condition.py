# -*- coding: utf-8 -*-
# 2021/2/21
# create by: snower


from .valuer import Valuer

class IfValuer(Valuer):
    def __init__(self, true_valuer, false_valuer, value_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.true_valuer = true_valuer
        self.false_valuer = false_valuer
        self.value_valuer = value_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(IfValuer, self).__init__(*args, **kwargs)

    def init_valuer(self):
        self.value_wait_loaded = True if self.value_valuer and self.value_valuer.require_loaded() else False
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        true_valuer = self.true_valuer.clone()
        false_valuer = self.false_valuer.clone() if self.false_valuer else None
        value_valuer = self.value_valuer.clone() if self.value_valuer else None
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(true_valuer, false_valuer, value_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, value_wait_loaded=self.value_wait_loaded, wait_loaded=self.wait_loaded)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.value_valuer:
            self.value_valuer.fill(data)
            if self.value_wait_loaded:
                self.true_valuer.fill(data)
                if self.false_valuer:
                    self.false_valuer.fill(data)
                return self
            self.value = self.value_valuer.get()
        else:
            self.value = data

        if self.value:
            self.true_valuer.fill(data)
        elif self.false_valuer:
            self.false_valuer.fill(data)

        if self.wait_loaded:
            if self.value:
                self.do_filter(self.true_valuer.get())
            elif self.false_valuer:
                self.do_filter(self.false_valuer.get())
        return self

    def get(self):
        if self.value_valuer and self.value_wait_loaded:
            self.value = self.value_valuer.get()
        elif self.wait_loaded:
            return self.return_valuer.get()

        if self.value:
            self.do_filter(self.true_valuer.get())
        elif self.false_valuer:
            self.do_filter(self.false_valuer.get())

        if self.return_valuer:
            self.return_valuer.fill(self.value)
            self.value = self.return_valuer.get()
        return self.value

    def childs(self):
        childs = [self.true_valuer]
        if self.false_valuer:
            childs.append(self.false_valuer)
        if self.value_valuer:
            childs.append(self.value_valuer)
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        fields = self.value_valuer.get_fields() if self.value_valuer else [self.key]
        for field in self.true_valuer.get_fields():
            fields.append(field)

        if self.false_valuer:
            for field in self.false_valuer.get_fields():
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

        true_filter = self.true_valuer.get_final_filter()
        if self.false_valuer:
            false_filter = self.false_valuer.get_final_filter()
            if false_filter is None:
                return true_filter

            if false_filter is not None and true_filter.__class__ != false_filter.__class__:
                return None
        return true_filter