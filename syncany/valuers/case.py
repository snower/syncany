# -*- coding: utf-8 -*-
# 18/8/8
# create by: snower

from .valuer import Valuer


class CaseValuer(Valuer):
    def __init__(self, case_valuers, default_case_valuer, value_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.case_valuers = case_valuers
        self.default_case_valuer = default_case_valuer
        self.value_valuer = value_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(CaseValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(CaseValuer, self).new_init()
        self.value_wait_loaded = True if self.value_valuer and self.value_valuer.require_loaded() else False
        self.case_wait_loaded = self.check_wait_loaded()
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False

    def clone_init(self, from_valuer):
        super(CaseValuer, self).clone_init(from_valuer)
        self.value_wait_loaded = from_valuer.value_wait_loaded
        self.case_wait_loaded = from_valuer.case_wait_loaded
        self.wait_loaded = from_valuer.wait_loaded

    def check_wait_loaded(self):
        for name, valuer in self.case_valuers.items():
            if valuer.require_loaded():
                return True
        if self.default_case_valuer and self.default_case_valuer.require_loaded():
            return True
        return False

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        case_valuers = {}
        for name, valuer in self.case_valuers.items():
            case_valuers[name] = valuer.clone(contexter, **kwargs)
        default_case_valuer = self.default_case_valuer.clone(contexter, **kwargs) if self.default_case_valuer else None
        value_valuer = self.value_valuer.clone(contexter, **kwargs) if self.value_valuer else None
        return_valuer = self.return_valuer.clone(contexter, **kwargs) if self.return_valuer else None
        if contexter is not None:
            return ContextCaseValuer(case_valuers, default_case_valuer, value_valuer, return_valuer, inherit_valuers,
                                     self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextCaseValuer):
            return ContextCaseValuer(case_valuers, default_case_valuer, value_valuer, return_valuer, inherit_valuers,
                                     self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(case_valuers, default_case_valuer, value_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.value_valuer:
            if self.value_wait_loaded:
                self.value_valuer.fill(data)
                for case_key, case_valuer in self.case_valuers.items():
                    case_valuer.fill(data)
                if self.default_case_valuer:
                    self.default_case_valuer.fill(data)
                return self
            value = self.value_valuer.fill_get(data)
        else:
            value = data

        if not self.case_wait_loaded or self.wait_loaded:
            if value in self.case_valuers:
                value = self.do_filter(self.case_valuers[value].fill_get(data))
            elif self.default_case_valuer:
                value = self.do_filter(self.default_case_valuer.fill_get(data))
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

        if value in self.case_valuers:
            self.case_valuers[value].fill(data)
        elif self.default_case_valuer:
            self.default_case_valuer.fill(data)
        self.value = value
        return self

    def get(self):
        if self.value_valuer and self.value_wait_loaded:
            value = self.value_valuer.get()
        elif not self.case_wait_loaded or self.wait_loaded:
            if self.return_valuer:
                if not self.wait_loaded:
                    return self.value
                return self.return_valuer.get()
            return self.value
        else:
            value = self.value

        if value in self.case_valuers:
            value = self.do_filter(self.case_valuers[value].get())
        elif self.default_case_valuer:
            value = self.do_filter(self.default_case_valuer.get())
        else:
            value = self.do_filter(None)
        if self.return_valuer:
            return self.return_valuer.fill_get(value)
        return value

    def fill_get(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        value = self.value_valuer.fill_get(data) if self.value_valuer else data
        if value in self.case_valuers:
            value = self.do_filter(self.case_valuers[value].fill_get(data))
        elif self.default_case_valuer:
            value = self.do_filter(self.default_case_valuer.fill_get(data))
        else:
            value = self.do_filter(None)
        if self.return_valuer:
            return self.return_valuer.fill_get(value)
        return value

    def childs(self):
        childs = list(self.case_valuers.values())
        if self.default_case_valuer:
            childs.append(self.default_case_valuer)
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
        for _, valuer in self.case_valuers.items():
            for field in valuer.get_fields():
                fields.append(field)

        if self.default_case_valuer:
            for field in self.default_case_valuer.get_fields():
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
        for _, valuer in self.case_valuers.items():
            filter = valuer.get_final_filter()
            if filter is None:
                continue

            if final_filter is not None and final_filter.__class__ != filter.__class__:
                return None
            final_filter = filter

        if self.default_case_valuer:
            filter = self.default_case_valuer.get_final_filter()
            if filter is None:
                return final_filter

            if final_filter is not None and final_filter.__class__ != filter.__class__:
                return None
        return final_filter


class ContextCaseValuer(CaseValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextCaseValuer, self).__init__(*args, **kwargs)

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
