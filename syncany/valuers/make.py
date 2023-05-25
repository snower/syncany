# -*- coding: utf-8 -*-
# 2020/6/29
# create by: snower

from .valuer import Valuer


class MakeValuer(Valuer):
    def __init__(self, value_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.value_valuer = value_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(MakeValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(MakeValuer, self).new_init()
        self.value_wait_loaded = self.check_wait_loaded()
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False

    def clone_init(self, from_valuer):
        super(MakeValuer, self).clone_init(from_valuer)
        self.value_wait_loaded = from_valuer.value_wait_loaded
        self.wait_loaded = from_valuer.wait_loaded

    def check_wait_loaded(self):
        if isinstance(self.value_valuer, dict):
            for _, (key_valuer, value_valuer) in self.value_valuer.items():
                if key_valuer.require_loaded():
                    return True
                if value_valuer.require_loaded():
                    return True
        elif isinstance(self.value_valuer, list):
            for value_valuer in self.value_valuer:
                if value_valuer.require_loaded():
                    return True
        elif isinstance(self.value_valuer, Valuer):
            if self.value_valuer.require_loaded():
                return True
        return False

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_loader(self, is_return_getter=True, **kwargs):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_loader(is_return_getter=False, **kwargs)
        if isinstance(self.value_valuer, dict):
            for key, (key_valuer, value_valuer) in self.value_valuer.items():
                key_valuer.mount_loader(is_return_getter=False, **kwargs)
                value_valuer.mount_loader(is_return_getter=False, **kwargs)
        elif isinstance(self.value_valuer, list):
            for valuer in self.value_valuer:
                valuer.mount_loader(is_return_getter=False, **kwargs)
        elif isinstance(self.value_valuer, Valuer):
            self.value_valuer.mount_loader(is_return_getter=False, **kwargs)
        if self.return_valuer:
            self.return_valuer.mount_loader(is_return_getter=is_return_getter and True, **kwargs)

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        if isinstance(self.value_valuer, dict):
            value_valuer = {key: (key_valuer.clone(contexter, **kwargs), value_valuer.clone(contexter, **kwargs))
                            for key, (key_valuer, value_valuer) in self.value_valuer.items()}
        elif isinstance(self.value_valuer, list):
            value_valuer = [valuer.clone(contexter, **kwargs) for valuer in self.value_valuer]
        elif isinstance(self.value_valuer, Valuer):
            value_valuer = self.value_valuer.clone(contexter, **kwargs)
        else:
            value_valuer = None
        return_valuer = self.return_valuer.clone(contexter, **kwargs) if self.return_valuer else None
        if contexter is not None:
            return ContextMakeValuer(value_valuer, return_valuer, inherit_valuers,
                                     self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextMakeValuer):
            return ContextMakeValuer(value_valuer, return_valuer, inherit_valuers,
                                     self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(value_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if not self.value_wait_loaded:
            if isinstance(self.value_valuer, dict):
                value = {key_valuer.fill_get(data): value_valuer.fill_get(data)
                         for key, (key_valuer, value_valuer) in self.value_valuer.items()}
            elif isinstance(self.value_valuer, list):
                value = [value_valuer.fill_get(data) for value_valuer in self.value_valuer]
                if len(value) == 1 and isinstance(value[0], list):
                    value = value[0]
            elif isinstance(self.value_valuer, Valuer):
                value = self.do_filter(self.value_valuer.fill_get(data))
            else:
                value = self.do_filter(None)
            if self.return_valuer:
                if not self.wait_loaded:
                    self.value = self.return_valuer.fill_get(value)
                else:
                    self.return_valuer.fill(value)
            else:
                self.value = value
            return self

        if isinstance(self.value_valuer, dict):
            for _, (key_valuer, value_valuer) in self.value_valuer.items():
                key_valuer.fill(data)
                value_valuer.fill(data)
        elif isinstance(self.value_valuer, list):
            for value_valuer in self.value_valuer:
                value_valuer.fill(data)
        elif isinstance(self.value_valuer, Valuer):
            self.value_valuer.fill(data)
        return self

    def get(self):
        if self.value_wait_loaded:
            if isinstance(self.value_valuer, dict):
                value = {key_valuer.get(): value_valuer.get()
                         for key, (key_valuer, value_valuer) in self.value_valuer.items()}
            elif isinstance(self.value_valuer, list):
                value = [value_valuer.get() for value_valuer in self.value_valuer]
                if len(value) == 1 and isinstance(value[0], list):
                    value = value[0]
            elif isinstance(self.value_valuer, Valuer):
                value = self.do_filter(self.value_valuer.get())
            else:
                value = self.do_filter(None)
            if self.return_valuer:
                return self.return_valuer.fill_get(value)
            return value
        if self.return_valuer:
            if not self.wait_loaded:
                return self.value
            return self.return_valuer.get()
        return self.value

    def fill_get(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if isinstance(self.value_valuer, dict):
            value = {key_valuer.fill_get(data): value_valuer.fill_get(data)
                     for key, (key_valuer, value_valuer) in self.value_valuer.items()}
        elif isinstance(self.value_valuer, list):
            value = [value_valuer.fill_get(data) for value_valuer in self.value_valuer]
            if len(value) == 1 and isinstance(value[0], list):
                value = value[0]
        elif isinstance(self.value_valuer, Valuer):
            value = self.do_filter(self.value_valuer.fill_get(data))
        else:
            value = self.do_filter(None)
        if self.return_valuer:
            return self.return_valuer.fill_get(value)
        return value

    def childs(self):
        childs = []
        if isinstance(self.value_valuer, dict):
            for _, (key_valuer, value_valuer) in self.value_valuer.items():
                childs.append(key_valuer)
                childs.append(value_valuer)
        elif isinstance(self.value_valuer, list):
            for value_valuer in self.value_valuer:
                childs.append(value_valuer)
        elif isinstance(self.value_valuer, Valuer):
            childs.append(self.value_valuer)
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            childs.extend(self.inherit_valuers)
        return childs

    def get_fields(self):
        fields = []
        if isinstance(self.value_valuer, dict):
            for _, (key_valuer, value_valuer) in self.value_valuer.items():
                for field in key_valuer.get_fields():
                    fields.append(field)
                for field in value_valuer.get_fields():
                    fields.append(field)
        elif isinstance(self.value_valuer, list):
            for value_valuer in self.value_valuer:
                for field in value_valuer.get_fields():
                    fields.append(field)
        elif isinstance(self.value_valuer, Valuer):
            for field in self.value_valuer.get_fields():
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

        if isinstance(self.value_valuer, Valuer):
            return self.value_valuer.get_final_filter()

        return None


class ContextMakeValuer(MakeValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextMakeValuer, self).__init__(*args, **kwargs)

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
