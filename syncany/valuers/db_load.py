# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .data import Valuer


class DBLoadValuer(Valuer):
    def __init__(self, loader, foreign_keys, foreign_filters, intercept_valuer, return_valuer,
                 inherit_valuers, *args, **kwargs):
        self.loader = loader
        self.foreign_keys = foreign_keys
        self.intercept_valuer = intercept_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.foreign_filters = foreign_filters
        super(DBLoadValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(DBLoadValuer, self).new_init()
        self.require_yield_values = False
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False

    def clone_init(self, from_valuer):
        super(DBLoadValuer, self).clone_init(from_valuer)
        self.require_yield_values = from_valuer.require_yield_values
        self.wait_loaded = from_valuer.wait_loaded

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_loader(self, is_return_getter=True, db_load_valuers=None, loader=None, **kwargs):
        self.loader.primary_loader = loader
        if is_return_getter:
            self.require_yield_values = True
        if db_load_valuers is None:
            db_load_valuers = []
        db_load_valuers.append(self)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_loader(is_return_getter=False, db_load_valuers=db_load_valuers, **kwargs)
        if self.intercept_valuer:
            self.intercept_valuer.mount_loader(is_return_getter=False, db_load_valuers=db_load_valuers, **kwargs)
        if self.return_valuer:
            self.return_valuer.mount_loader(is_return_getter=is_return_getter and True, db_load_valuers=db_load_valuers, **kwargs)

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        intercept_valuer = self.intercept_valuer.clone(contexter, **kwargs) if self.intercept_valuer else None
        return_valuer = self.return_valuer.clone(contexter, **kwargs)
        if contexter is not None:
            return ContextDBLoadValuer(self.loader, self.foreign_keys, self.foreign_filters, intercept_valuer,
                                       return_valuer, inherit_valuers, self.key, self.filter, from_valuer=self,
                                       contexter=contexter)
        if isinstance(self, ContextDBLoadValuer):
            return ContextDBLoadValuer(self.loader, self.foreign_keys, self.foreign_filters, intercept_valuer,
                                       return_valuer, inherit_valuers, self.key, self.filter, from_valuer=self,
                                       contexter=self.contexter)
        return self.__class__(self.loader, self.foreign_keys, self.foreign_filters, intercept_valuer,
                              return_valuer, inherit_valuers, self.key, self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if not self.wait_loaded:
            return self

        if self.intercept_valuer:
            datas, values = self.loader.get(), []
            for data in datas:
                intercept_result = self.intercept_valuer.fill_get(data)
                if intercept_result is not None and not intercept_result:
                    continue
                values.append(data)
        else:
            values = self.loader.get()
        if not self.require_yield_values:
            if len(values) == 1:
                values = values[0] if values else None
            self.return_valuer.fill(values)
            return self
        self.value = [self.return_valuer.clone().fill(value) for value in values]
        return self

    def get(self):
        if not self.wait_loaded:
            if self.intercept_valuer:
                datas, values = self.loader.get(), []
                for data in datas:
                    intercept_result = self.intercept_valuer.fill_get(data)
                    if intercept_result is not None and not intercept_result:
                        continue
                    values.append(data)
            else:
                values = self.loader.get()
            if not self.require_yield_values:
                if len(values) == 1:
                    values = values[0] if values else None
                return self.return_valuer.fill_get(values)
            values = [self.return_valuer.fill_get(value) for value in values]
        else:
            if not self.require_yield_values:
                return self.return_valuer.get()
            values = [valuer.get() for valuer in self.value]
        if len(values) == 1 and values[0] is not None:
            return values[0]

        def gen_iter(iter_datas):
            yield None
            for value in iter_datas:
                if value is None:
                    continue
                yield value
        g = gen_iter(values)
        g.send(None)
        return g

    def fill_get(self, data):
        return self.fill(data).get()

    def childs(self):
        valuers = []
        if self.intercept_valuer:
            valuers.append(self.intercept_valuer)
        if self.return_valuer:
            valuers.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                valuers.append(inherit_valuer)
        return valuers

    def get_fields(self):
        fields = []
        if self.intercept_valuer:
            for field in self.intercept_valuer.get_fields():
                fields.append(field)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        if self.return_valuer:
            return self.return_valuer.get_final_filter()
        return None

    def require_loaded(self):
        return True


class ContextDBLoadValuer(DBLoadValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextDBLoadValuer, self).__init__(*args, **kwargs)

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

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if not self.wait_loaded:
            return self

        if self.intercept_valuer:
            datas, values = self.loader.get(), []
            for data in datas:
                intercept_result = self.intercept_valuer.fill_get(data)
                if intercept_result is not None and not intercept_result:
                    continue
                values.append(data)
        else:
            values = self.loader.get()
        if not self.require_yield_values:
            if len(values) == 1:
                values = values[0] if values else None
            self.return_valuer.fill(values)
            return self

        contexter_values, value_valuers = self.contexter.values, []
        for value in values:
            self.return_valuer.contexter.values = self.contexter.create_inherit_values(contexter_values)
            self.return_valuer.fill(value)
            value_valuers.append((self.return_valuer, self.return_valuer.contexter.values))
        self.value = value_valuers
        return self

    def get(self):
        if not self.wait_loaded:
            if self.intercept_valuer:
                datas, values = self.loader.get(), []
                for data in datas:
                    intercept_result = self.intercept_valuer.fill_get(data)
                    if intercept_result is not None and not intercept_result:
                        continue
                    values.append(data)
            else:
                values = self.loader.get()
            if not self.require_yield_values:
                if len(values) == 1:
                    values = values[0] if values else None
                return self.return_valuer.fill_get(values)
            values = [self.return_valuer.fill_get(value) for value in values]
        else:
            if not self.require_yield_values:
                return self.return_valuer.get()
            value_valuers, values = self.value, []
            for valuer, contexter_values in value_valuers:
                valuer.contexter.values = contexter_values
                values.append(valuer.get())
        if len(values) == 1 and values[0] is not None:
            return values[0]

        def gen_iter(iter_datas):
            yield None
            for value in iter_datas:
                if value is None:
                    continue
                yield value
        g = gen_iter(values)
        g.send(None)
        return g
