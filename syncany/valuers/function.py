# -*- coding: utf-8 -*-
# 2021/2/4
# create by: snower

from .valuer import Valuer


class LambdaFunction(object):
    def __init__(self, calculate_valuer):
        self.calculate_valuer = calculate_valuer

    def __call__(self, data):
        calculate_valuer = self.calculate_valuer.clone()
        calculate_valuer.fill(data)
        return calculate_valuer.get()


class LambdaValuer(Valuer):
    def __init__(self, calculate_valuer, inherit_valuers, *args, **kwargs):
        self.calculate_valuer = calculate_valuer
        self.inherit_valuers = inherit_valuers
        super(LambdaValuer, self).__init__(*args, **kwargs)

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self, contexter=None):
        calculate_valuer = self.calculate_valuer.clone(contexter) if self.calculate_valuer else None
        inherit_valuers = [inherit_valuer.clone(contexter) for inherit_valuer in self.inherit_valuers] \
            if self.inherit_valuers else None
        if contexter is not None:
            return ContextLambdaValuer(calculate_valuer, inherit_valuers, self.key, self.filter, from_valuer=self,
                                       contexter=contexter)
        if isinstance(self, ContextLambdaValuer):
            return ContextLambdaValuer(calculate_valuer, inherit_valuers, self.key, self.filter, from_valuer=self,
                                       contexter=self.contexter)
        return self.__class__(calculate_valuer, inherit_valuers, self.key, self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)
        return self

    def get(self):
        return LambdaFunction(self.calculate_valuer)

    def childs(self):
        childs = []
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        fields = []
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        return None


class ContextLambdaValuer(LambdaValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextLambdaValuer, self).__init__(*args, **kwargs)

    @property
    def value(self):
        try:
            return self.contexter.values[self.value_context_id]
        except KeyError:
            return None

    @value.setter
    def value(self, v):
        if v is None:
            if self.value_context_id in self.contexter.values:
                self.contexter.values.pop(self.value_context_id)
            return
        self.contexter.values[self.value_context_id] = v
