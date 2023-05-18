# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from .valuer import Valuer


class CalculateValuer(Valuer):
    def __init__(self, calculater, args_valuers, return_valuer, inherit_valuers, *args, **kwargs):
        self.calculater = calculater
        self.args_valuers = args_valuers
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(CalculateValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(CalculateValuer, self).new_init()
        self.wait_loaded = False
        for valuer in self.args_valuers:
            if valuer.require_loaded():
                self.wait_loaded = True
                break

    def clone_init(self, from_valuer):
        super(CalculateValuer, self).clone_init(from_valuer)
        self.wait_loaded = from_valuer.wait_loaded

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self, contexter=None):
        args_valuers = []
        for valuer in self.args_valuers:
            args_valuers.append(valuer.clone(contexter))
        return_valuer = self.return_valuer.clone(contexter) if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone(contexter) for inherit_valuer in self.inherit_valuers] \
            if self.inherit_valuers else None
        if contexter is not None:
            return ContextCalculateValuer(self.calculater, args_valuers, return_valuer, inherit_valuers,
                                          self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextCalculateValuer):
            return ContextCalculateValuer(self.calculater, args_valuers, return_valuer, inherit_valuers,
                                          self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(self.calculater, args_valuers, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        for valuer in self.args_valuers:
            valuer.fill(data)

        if not self.wait_loaded:
            values = [valuer.get() for valuer in self.args_valuers]
            if self.return_valuer:
                self.return_valuer.fill(self.do_filter(self.calculater.calculate(*values)))
            else:
                self.value = self.do_filter(self.calculater.calculate(*values))
        return self

    def get(self):
        if self.wait_loaded:
            values = [valuer.get() for valuer in self.args_valuers]
            if self.return_valuer:
                return self.return_valuer.fill(self.do_filter(self.calculater.calculate(*values))).get()
            return self.do_filter(self.calculater.calculate(*values))
        if self.return_valuer:
            return self.return_valuer.get()
        return self.value

    def childs(self):
        childs = []
        if self.args_valuers:
            for args_valuer in self.args_valuers:
                childs.append(args_valuer)
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        fields = []
        for valuer in self.args_valuers:
            for field in valuer.get_fields():
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

        final_filter = None
        for valuer in self.args_valuers:
            filter = valuer.get_final_filter()
            if filter is None:
                continue

            if final_filter is not None and final_filter.__class__ != filter.__class__:
                return None
            final_filter = filter
        return final_filter


class ContextCalculateValuer(CalculateValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextCalculateValuer, self).__init__(*args, **kwargs)

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
