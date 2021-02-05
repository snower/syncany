# -*- coding: utf-8 -*-
# 2021/2/5
# create by: snower

from .valuer import Valuer, LoadAllFieldsException
from ..filters import ArrayFilter


class BreakReturn(Exception):
    NULL = object()

    def __init__(self, value=NULL, *args, **kwargs):
        super(BreakReturn, self).__init__(*args, **kwargs)
        self.value = value

    def get(self):
        return self.value


class ContinueReturn(Exception):
    NULL = object()

    def __init__(self, value=NULL, *args, **kwargs):
        super(ContinueReturn, self).__init__(*args, **kwargs)
        self.value = value

    def get(self):
        return self.value


class ForeachValuer(Valuer):
    def __init__(self, value_valuer, calculate_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        super(ForeachValuer, self).__init__(*args, **kwargs)

        self.value_valuer = value_valuer
        self.calculate_valuer = calculate_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.value_wait_loaded = False if not self.value_valuer else self.value_valuer.require_loaded()
        self.calculate_wait_loaded = True if self.value_wait_loaded or not self.return_valuer or \
                                             (self.calculate_valuer and
                                              self.calculate_valuer.require_loaded()) else False
        self.calculated_values = []

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        value_valuer = self.value_valuer.clone() if self.value_valuer else None
        calculate_valuer = self.calculate_valuer.clone() if self.calculate_valuer else None
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(value_valuer, calculate_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.value_valuer:
            self.value_valuer.fill(data)
            if not self.value_wait_loaded:
                self.value = self.value_valuer.get()
        else:
            self.value = data

        if not self.value_wait_loaded:
            if isinstance(self.value, dict):
                for k, v in self.value.items():
                    calculate_valuer = self.calculate_valuer.clone()
                    calculate_valuer.fill(dict(_key_=k, **v))
                    self.calculated_values.append(calculate_valuer)
            elif isinstance(self.value, (list, tuple, set)):
                for i in range(len(self.value)):
                    calculate_valuer = self.calculate_valuer.clone()
                    calculate_valuer.fill(dict(_index_=i, **self.value[i]))
                    self.calculated_values.append(calculate_valuer)

            if not self.calculate_wait_loaded:
                values = []
                for valuer in self.calculated_values:
                    try:
                        values.append(valuer.get())
                    except ContinueReturn as e:
                        if e.value != ContinueReturn.NULL:
                            values.append(e.value)
                        continue
                    except BreakReturn as e:
                        if e.value != BreakReturn.NULL:
                            values.append(e.value)
                        break
                self.value = values
                if self.return_valuer:
                    self.return_valuer.fill(self.value)
        return self

    def get(self):
        if self.value_valuer and self.value_wait_loaded:
            self.value = self.value_valuer.get()

        if self.value_wait_loaded:
            if isinstance(self.value, dict):
                for k, v in self.value.items():
                    calculate_valuer = self.calculate_valuer.clone()
                    calculate_valuer.fill(dict(_key_=k, **v))
                    self.calculated_values.append(calculate_valuer)
            elif isinstance(self.value, (list, tuple, set)):
                for i in range(len(self.value)):
                    calculate_valuer = self.calculate_valuer.clone()
                    calculate_valuer.fill(dict(_index_=i, **self.value[i]))
                    self.calculated_values.append(calculate_valuer)

        if self.calculate_wait_loaded:
            values = []
            for valuer in self.calculated_values:
                try:
                    values.append(valuer.get())
                except ContinueReturn as e:
                    if e.value != ContinueReturn.NULL:
                        values.append(e.value)
                    continue
                except BreakReturn as e:
                    if e.value != BreakReturn.NULL:
                        values.append(e.value)
                    break
            self.value = values
            if self.return_valuer:
                self.return_valuer.fill(self.value)

        if self.return_valuer:
            return self.return_valuer.get()
        return self.value

    def childs(self):
        childs = []
        if self.value_valuer:
            childs.append(self.value_valuer)
        if self.calculate_valuer:
            childs.append(self.calculate_valuer)
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        is_pass, fields = False, []
        try:
            if self.value_valuer:
                for field in self.value_valuer.get_fields():
                    fields.append(field)
        except LoadAllFieldsException:
            is_pass = True

        if (not self.value_valuer or is_pass) and self.calculate_valuer:
            for field in self.calculate_valuer.get_fields():
                fields.append(field)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        if self.return_valuer:
            return self.return_valuer.get_final_filter()
        return ArrayFilter()


class BreakValuer(Valuer):
    def __init__(self, value_valuer, *args, **kwargs):
        super(BreakValuer, self).__init__(*args, **kwargs)

        self.value_valuer = value_valuer

    def clone(self):
        value_valuer = self.value_valuer.clone() if self.value_valuer else None
        return self.__class__(value_valuer, self.key, self.filter)

    def fill(self, data):
        self.value_valuer.fill(data)
        return self

    def get(self):
        if self.value_valuer:
            raise BreakReturn(self.value_valuer.get())
        raise BreakReturn()

    def childs(self):
        childs = []
        if self.value_valuer:
            childs.append(self.value_valuer)
        return childs

    def get_fields(self):
        fields = []
        if self.value_valuer:
            for field in self.value_valuer.get_fields():
                fields.append(field)
        return fields

    def get_final_filter(self):
        if self.value_valuer:
            return self.value_valuer.get_final_filter()
        return None


class ContinueValuer(Valuer):
    def __init__(self, value_valuer, *args, **kwargs):
        super(ContinueValuer, self).__init__(*args, **kwargs)

        self.value_valuer = value_valuer

    def clone(self):
        value_valuer = self.value_valuer.clone() if self.value_valuer else None
        return self.__class__(value_valuer, self.key, self.filter)

    def fill(self, data):
        self.value_valuer.fill(data)
        return self

    def get(self):
        if self.value_valuer:
            raise ContinueReturn(self.value_valuer.get())
        raise ContinueReturn()

    def childs(self):
        childs = []
        if self.value_valuer:
            childs.append(self.value_valuer)
        return childs

    def get_fields(self):
        fields = []
        if self.value_valuer:
            for field in self.value_valuer.get_fields():
                fields.append(field)
        return fields

    def get_final_filter(self):
        if self.value_valuer:
            return self.value_valuer.get_final_filter()
        return None