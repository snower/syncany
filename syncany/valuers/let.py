# -*- coding: utf-8 -*-
# 2020/7/1
# create by: snower

from .valuer import Valuer


class LetValuer(Valuer):
    filled_data = None

    def __init__(self, key_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.key_valuer = key_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(LetValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(LetValuer, self).new_init()
        self.wait_loaded = True if not self.return_valuer else False
        if self.return_valuer:
            self.check_wait_loaded()

    def clone_init(self, from_valuer):
        super(LetValuer, self).clone_init(from_valuer)
        self.wait_loaded = from_valuer.wait_loaded

    def check_wait_loaded(self):
        if self.key_valuer.require_loaded():
            self.wait_loaded = True

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self, contexter=None):
        key_valuer = self.key_valuer.clone(contexter) if self.key_valuer else None
        return_valuer = self.return_valuer.clone(contexter) if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone(contexter) for inherit_valuer in self.inherit_valuers] \
            if self.inherit_valuers else None
        if contexter is not None:
            return ContextLetValuer(key_valuer, return_valuer, inherit_valuers,
                                    self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextLetValuer):
            return ContextLetValuer(key_valuer, return_valuer, inherit_valuers,
                                    self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(key_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)

    def reinit(self):
        self.filled_data = None
        return super(LetValuer, self).reinit()

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        self.key_valuer.fill(data)

        if not self.wait_loaded:
            self.key = self.key_valuer.get()
            super(LetValuer, self).fill(data)
            if self.return_valuer:
                self.return_valuer.fill(super(LetValuer, self).get())
        else:
            self.filled_data = data
        return self

    def get(self):
        if self.wait_loaded:
            self.key = self.key_valuer.get()
            super(LetValuer, self).fill(self.filled_data)
            self.filled_data = None
            if self.return_valuer:
                self.return_valuer.fill(super(LetValuer, self).get())
                return self.return_valuer.get()
        return super(LetValuer, self).get()

    def childs(self):
        childs = []
        if self.key_valuer:
            childs.append(self.key_valuer)
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        fields = []
        for field in self.key_valuer.get_fields():
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


class ContextLetValuer(LetValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        self.filled_data_context_id = (id(self), "filled_data")
        super(ContextLetValuer, self).__init__(*args, **kwargs)

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

    @property
    def filled_data(self):
        try:
            return self.contexter.values[self.filled_data_context_id]
        except KeyError:
            return None

    @filled_data.setter
    def filled_data(self, v):
        if v is None:
            if self.filled_data_context_id in self.contexter.values:
                self.contexter.values.pop(self.filled_data_context_id)
            return
        self.contexter.values[self.filled_data_context_id] = v
