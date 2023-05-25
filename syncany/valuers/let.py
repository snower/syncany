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
        self.key_wait_loaded = True if self.key_valuer and self.key_valuer.require_loaded() else False
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False

    def clone_init(self, from_valuer):
        super(LetValuer, self).clone_init(from_valuer)
        self.key_wait_loaded = from_valuer.key_wait_loaded
        self.wait_loaded = from_valuer.wait_loaded

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_loader(self, is_return_getter=True, **kwargs):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_loader(is_return_getter=False, **kwargs)
        if self.key_valuer:
            self.key_valuer.mount_loader(is_return_getter=False, **kwargs)
        if self.return_valuer:
            self.return_valuer.mount_loader(is_return_getter=is_return_getter and True, **kwargs)

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        key_valuer = self.key_valuer.clone(contexter, **kwargs) if self.key_valuer else None
        return_valuer = self.return_valuer.clone(contexter, **kwargs) if self.return_valuer else None
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

        if not self.key_wait_loaded:
            self.key = self.key_valuer.fill_get(data)
            super(LetValuer, self).fill(data)
            if self.return_valuer:
                if not self.wait_loaded:
                    self.value = self.return_valuer.fill_get(super(LetValuer, self).get())
                else:
                    self.return_valuer.fill(super(LetValuer, self).get())
        else:
            self.key_valuer.fill(data)
            self.filled_data = data
        return self

    def get(self):
        if self.key_wait_loaded:
            self.key = self.key_valuer.get()
            super(LetValuer, self).fill(self.filled_data)
            self.filled_data = None
            if self.return_valuer:
                return self.return_valuer.fill_get(super(LetValuer, self).get())
            return super(LetValuer, self).get()
        if self.return_valuer:
            if not self.wait_loaded:
                return self.value
            return self.return_valuer.get()
        return super(LetValuer, self).get()

    def fill_get(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        self.key = self.key_valuer.fill_get(data)
        if self.return_valuer:
            return self.return_valuer.fill_get(super(LetValuer, self).fill_get(data))
        return super(LetValuer, self).fill_get(data)

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
