# -*- coding: utf-8 -*-
# 18/8/8
# create by: snower

from .valuer import Valuer

class CaseValuer(Valuer):
    def __init__(self, case_valuers, default_case_valuer, value_valuer, *args, **kwargs):
        super(CaseValuer, self).__init__(*args, **kwargs)

        self.case_valuers = case_valuers
        self.default_case_valuer = default_case_valuer
        self.value_valuer = value_valuer or None

    def clone(self):
        case_valuers = {}
        for name, valuer in self.case_valuers.items():
            case_valuers[name] = valuer.clone()
        default_case_valuer = self.default_case_valuer.clone() if self.default_case_valuer else None
        value_valuer = self.value_valuer.clone() if self.value_valuer else None
        return self.__class__(case_valuers, default_case_valuer, value_valuer, self.key, self.filter)

    def fill(self, data):
        super(CaseValuer, self).fill(data)

        if self.value_valuer:
            self.value_valuer.fill(data)
            for case_key, case_valuer in self.case_valuers.items():
                case_valuer.fill(data)
            if self.default_case_valuer:
                self.default_case_valuer.fill(data)
        else:
            if self.value in self.case_valuers:
                self.case_valuers[self.value].fill(data)
            elif self.default_case_valuer:
                self.default_case_valuer.fill(data)
        return self

    def get(self):
        if self.value_valuer:
            self.value = self.value_valuer.get()

        if self.value in self.case_valuers:
            return self.case_valuers[self.value].get()
        elif self.default_case_valuer:
            return self.default_case_valuer.get()
        return self.value

    def childs(self):
        return list(self.case_valuers.values()) \
               + ([self.default_case_valuer] if self.default_case_valuer else []) \
               + ([self.value_valuer] if self.value_valuer else [])

    def get_fields(self):
        fields = self.value_valuer.get_fields() if self.value_valuer else [self.key]
        for _, valuer in self.case_valuers.items():
            for field in valuer.get_fields():
                fields.append(field)

        if self.default_case_valuer:
            for field in self.default_case_valuer.get_fields():
                fields.append(field)
        return fields

    def get_final_filter(self):
        final_filter = None
        for valuer in self.childs():
            filter = valuer.get_final_filter()
            if filter is None:
                continue

            if final_filter is not None and final_filter.__class__ != filter.__class__:
                return None

            final_filter = filter

        return final_filter