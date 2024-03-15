# -*- coding: utf-8 -*-
# 2020/7/3
# create by: snower

from .valuer import Valuer


class AssignValuer(Valuer):
    def __init__(self, global_value, calculate_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.global_value = global_value
        self.calculate_valuer = calculate_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(AssignValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(AssignValuer, self).new_init()
        self.calculate_wait_loaded = True if self.calculate_valuer and self.calculate_valuer.require_loaded() else False
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False

    def clone_init(self, from_valuer):
        super(AssignValuer, self).clone_init(from_valuer)
        self.calculate_wait_loaded = from_valuer.calculate_wait_loaded
        self.wait_loaded = from_valuer.wait_loaded

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_loader(self, is_return_getter=True, **kwargs):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_loader(is_return_getter=False, **kwargs)
        if self.calculate_valuer:
            self.calculate_valuer.mount_loader(is_return_getter=False, **kwargs)
        if self.return_valuer:
            self.return_valuer.mount_loader(is_return_getter=is_return_getter and True, **kwargs)

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        calculate_valuer = self.calculate_valuer.clone(contexter, **kwargs) if self.calculate_valuer else None
        return_valuer = self.return_valuer.clone(contexter, **kwargs) if self.return_valuer else None
        if contexter is not None:
            return ContextAssignValuer(self.global_value, calculate_valuer, return_valuer, inherit_valuers,
                                       self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextAssignValuer):
            return ContextAssignValuer(self.global_value, calculate_valuer, return_valuer, inherit_valuers,
                                       self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(self.global_value, calculate_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.calculate_valuer:
            if not self.calculate_wait_loaded:
                value = self.do_filter(self.calculate_valuer.fill_get(self.global_value))
                self.global_value[self.key] = value
                if self.return_valuer:
                    if not self.wait_loaded:
                        self.value = self.return_valuer.fill_get(value)
                    else:
                        self.return_valuer.fill(value)
                else:
                    self.value = value
            else:
                self.calculate_valuer.fill(self.global_value)
        elif self.return_valuer:
            value = self.do_filter(self.global_value.get(self.key, None))
            final_filter = self.return_valuer.get_final_filter()
            if final_filter:
                value = final_filter.filter(value)
            if not self.wait_loaded:
                self.value = self.return_valuer.fill_get(value)
            else:
                self.return_valuer.fill(value)
        else:
            self.value = self.do_filter(self.global_value.get(self.key, None))
        return self

    def get(self):
        if self.calculate_valuer:
            if self.calculate_wait_loaded:
                value = self.do_filter(self.calculate_valuer.get())
                self.global_value[self.key] = value
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

        if self.calculate_valuer:
            value = self.do_filter(self.calculate_valuer.fill_get(self.global_value))
            self.global_value[self.key] = value
            if self.return_valuer:
                return self.return_valuer.fill_get(value)
            return value
        if self.return_valuer:
            value = self.do_filter(self.global_value.get(self.key, None))
            final_filter = self.return_valuer.get_final_filter()
            if final_filter:
                value = final_filter.filter(value)
            return self.return_valuer.fill_get(value)
        return self.do_filter(self.global_value.get(self.key, None))

    def childs(self):
        childs = []
        if self.calculate_valuer:
            childs.append(self.calculate_valuer)
        if self.return_valuer:
            childs.append(self.return_valuer)
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
        if self.return_valuer:
            return self.calculate_valuer.get_final_filter()

        if self.filter:
            return self.filter

        if self.calculate_valuer:
            return self.calculate_valuer.get_final_filter()
        return None

    def is_const(self):
        return False


class ContextAssignValuer(AssignValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextAssignValuer, self).__init__(*args, **kwargs)

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
