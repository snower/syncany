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

    def init_valuer(self):
        self.value_wait_loaded = True if self.value_valuer and self.value_valuer.require_loaded() else False
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        case_valuers = {}
        for name, valuer in self.case_valuers.items():
            case_valuers[name] = valuer.clone()
        default_case_valuer = self.default_case_valuer.clone() if self.default_case_valuer else None
        value_valuer = self.value_valuer.clone() if self.value_valuer else None
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(case_valuers, default_case_valuer, value_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, value_wait_loaded=self.value_wait_loaded, wait_loaded=self.wait_loaded)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.value_valuer:
            self.value_valuer.fill(data)
            if self.value_wait_loaded:
                for case_key, case_valuer in self.case_valuers.items():
                    case_valuer.fill(data)
                if self.default_case_valuer:
                    self.default_case_valuer.fill(data)
                return self
            self.value = self.value_valuer.get()
        else:
            self.value = data

        if self.value in self.case_valuers:
            self.case_valuers[self.value].fill(data)
        elif self.default_case_valuer:
            self.default_case_valuer.fill(data)

        if self.wait_loaded:
            if self.value in self.case_valuers:
                self.do_filter(self.case_valuers[self.value].get())
            elif self.default_case_valuer:
                self.do_filter(self.default_case_valuer.get())
            self.return_valuer.fill(self.value)
        return self

    def get(self):
        if self.value_valuer and self.value_wait_loaded:
            self.value = self.value_valuer.get()
        elif self.wait_loaded:
            return self.return_valuer.get()

        if self.value in self.case_valuers:
            self.do_filter(self.case_valuers[self.value].get())
        elif self.default_case_valuer:
            self.do_filter(self.default_case_valuer.get())

        if self.return_valuer:
            self.return_valuer.fill(self.value)
            self.value = self.return_valuer.get()
        return self.value

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