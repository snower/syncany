# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from .valuer import Valuer


class SchemaValuer(Valuer):
    def __init__(self, schema_valuers, *args, **kwargs):
        self.schema_valuers = schema_valuers
        super(SchemaValuer, self).__init__(*args, **kwargs)

    def mount_scoper(self, scoper=None, is_return_getter=True,**kwargs):
        for name, valuer in self.schema_valuers.items():
            valuer.mount_scoper(scoper=scoper, is_return_getter=False,**kwargs)

    def clone(self, contexter=None, **kwargs):
        schema_valuers = {}
        for name, valuer in self.schema_valuers.items():
            schema_valuers[name] = valuer.clone(contexter, **kwargs)
        if contexter is not None:
            return ContextSchemaValuer(schema_valuers, self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextSchemaValuer):
            return ContextSchemaValuer(schema_valuers, self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(schema_valuers, self.key, self.filter, from_valuer=self)

    def fill(self, data):
        super(SchemaValuer, self).fill(data)

        for name, valuer in self.schema_valuers.items():
            valuer.fill(data)
        return self

    def get(self):
        value = {}
        for name, valuer in self.schema_valuers.items():
            value[name] = valuer.get()
        return value

    def fill_get(self, data):
        super(SchemaValuer, self).fill(data)

        value = {}
        for name, valuer in self.schema_valuers.items():
            value[name] = valuer.fill_get(data)
        return value

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


class ContextSchemaValuer(SchemaValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = id(self) * 10
        super(ContextSchemaValuer, self).__init__(*args, **kwargs)

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
