# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from .valuer import Valuer

class SchemaValuer(Valuer):
    def __init__(self, schema_valuers, *args, **kwargs):
        super(SchemaValuer, self).__init__(*args, **kwargs)

        self.schema_valuers = schema_valuers

    def clone(self):
        schema_valuers = {}
        for name, valuer in self.schema_valuers.items():
            schema_valuers[name] = valuer.clone()
        return self.__class__(schema_valuers, self.key, self.filter)

    def fill(self, data):
        super(SchemaValuer, self).fill(data)

        for name, valuer in self.schema_valuers.items():
            valuer.fill(data)
        return self

    def get(self):
        self.value = {}
        for name, valuer in self.schema_valuers.items():
            self.value[name] = valuer.get()

        return self.value

    def childs(self):
        return list(self.schema_valuers.values())

    def get_fields(self):
        fields = []
        for name, valuer in self.schema_valuers.items():
            for field in valuer.get_fields():
                fields.append(field)

        return fields

    def get_final_filter(self):
        return None