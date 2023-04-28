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

    def new_init(self):
        super(IfValuer, self).new_init()
        self.value_wait_loaded = True if self.value_valuer and self.value_valuer.require_loaded() else False
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False

    def clone_init(self, from_valuer):
        super(IfValuer, self).clone_init(from_valuer)
        self.value_wait_loaded = from_valuer.value_wait_loaded
        self.wait_loaded = from_valuer.wait_loaded

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self, contexter=None):
        true_valuer = self.true_valuer.clone(contexter)
        false_valuer = self.false_valuer.clone(contexter) if self.false_valuer else None
        value_valuer = self.value_valuer.clone(contexter) if self.value_valuer else None
        return_valuer = self.return_valuer.clone(contexter) if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone(contexter) for inherit_valuer in self.inherit_valuers] \
            if self.inherit_valuers else None
        if contexter is not None:
            return ContextIfValuer(true_valuer, false_valuer, value_valuer, return_valuer, inherit_valuers,
                                   self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextIfValuer):
            return ContextIfValuer(true_valuer, false_valuer, value_valuer, return_valuer, inherit_valuers,
                                   self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(true_valuer, false_valuer, value_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)

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
            value = self.value_valuer.get()
        else:
            value = data

        if self.wait_loaded:
            if value:
                value = self.do_filter(self.true_valuer.fill(data).get())
            elif self.false_valuer:
                value = self.do_filter(self.false_valuer.fill(data).get())
            else:
                value = self.do_filter(None)
            self.return_valuer.fill(value)
            return self

        if value:
            self.true_valuer.fill(data)
        elif self.false_valuer:
            self.false_valuer.fill(data)
        self.value = value
        return self

    def get(self):
        if self.value_valuer and self.value_wait_loaded:
            value = self.value_valuer.get()
        elif self.wait_loaded:
            return self.return_valuer.get()
        else:
            value = self.value

        if value:
            value = self.do_filter(self.true_valuer.get())
        elif self.false_valuer:
            value = self.do_filter(self.false_valuer.get())
        else:
            value = self.do_filter(None)
        if self.return_valuer:
            return self.return_valuer.fill(value).get()
        return value

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


class ContextIfValuer(IfValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextIfValuer, self).__init__(*args, **kwargs)

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
