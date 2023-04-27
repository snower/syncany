# -*- coding: utf-8 -*-
# 2021/2/5
# create by: snower

import types
from .valuer import Valuer, LoadAllFieldsException
from ..filters import ArrayFilter


range_type = type(range(1))


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
    calculated_values = None

    def __init__(self, value_valuer, calculate_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.value_valuer = value_valuer
        self.calculate_valuer = calculate_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(ForeachValuer, self).__init__(*args, **kwargs)

        self.calculated_values = []

    def new_init(self):
        super(ForeachValuer, self).new_init()
        self.value_wait_loaded = False if not self.value_valuer else self.value_valuer.require_loaded()
        self.calculate_wait_loaded = True if self.value_wait_loaded or not self.return_valuer or \
                                             (self.calculate_valuer and
                                              self.calculate_valuer.require_loaded()) else False

    def clone_init(self, from_valuer):
        super(ForeachValuer, self).clone_init(from_valuer)
        self.value_wait_loaded = from_valuer.value_wait_loaded
        self.calculate_wait_loaded = from_valuer.calculate_wait_loaded

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self, contexter=None):
        value_valuer = self.value_valuer.clone(contexter) if self.value_valuer else None
        calculate_valuer = self.calculate_valuer.clone(contexter) if self.calculate_valuer else None
        return_valuer = self.return_valuer.clone(contexter) if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone(contexter) for inherit_valuer in self.inherit_valuers] \
            if self.inherit_valuers else None
        if contexter is not None:
            return ContextForeachValuer(value_valuer, calculate_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextForeachValuer):
            return ContextForeachValuer(value_valuer, calculate_valuer, return_valuer, inherit_valuers,
                                        self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(value_valuer, calculate_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)
    
    def reinit(self):
        self.calculated_values = []
        return super(ForeachValuer, self).reinit()

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.value_valuer:
            self.value_valuer.fill(data)
            if not self.value_wait_loaded:
                value = self.value_valuer.get()
            else:
                value = data
        else:
            value = data

        if not self.value_wait_loaded:
            calculated_values = []
            if isinstance(value, dict):
                for k, v in value.items():
                    calculate_valuer = self.calculate_valuer.clone()
                    if isinstance(v, dict):
                        calculate_valuer.fill(dict(_index_=k, **v))
                    else:
                        calculate_valuer.fill(dict(_index_=k, _value_=v))
                    calculated_values.append(calculate_valuer)
            elif isinstance(value, (list, types.GeneratorType)):
                for i in range(len(value)):
                    calculate_valuer = self.calculate_valuer.clone()
                    if isinstance(value[i], dict):
                        calculate_valuer.fill(dict(_index_=i, **value[i]))
                    else:
                        calculate_valuer.fill(dict(_index_=i, _value_=value[i]))
                    calculated_values.append(calculate_valuer)
            elif isinstance(value, range_type):
                for i in value:
                    calculate_valuer = self.calculate_valuer.clone()
                    calculate_valuer.fill(dict(_index_=i))
                    calculated_values.append(calculate_valuer)

            if not self.calculate_wait_loaded:
                values = []
                for valuer in calculated_values:
                    try:
                        values.append(self.do_filter(valuer.get()))
                    except ContinueReturn as e:
                        if e.value != ContinueReturn.NULL:
                            values.append(e.value)
                        continue
                    except BreakReturn as e:
                        if e.value != BreakReturn.NULL:
                            values.append(e.value)
                        break
                if self.return_valuer:
                    self.return_valuer.fill(values)
                else:
                    self.value = values
            else:
                self.calculated_values = calculated_values
        return self

    def get(self):
        if self.value_valuer and self.value_wait_loaded:
            value = self.value_valuer.get()
        else:
            value = self.value

        if self.value_wait_loaded:
            calculated_values = self.calculated_values
            if isinstance(value, dict):
                for k, v in value.items():
                    calculate_valuer = self.calculate_valuer.clone()
                    if isinstance(v, dict):
                        calculate_valuer.fill(dict(_index_=k, **v))
                    else:
                        calculate_valuer.fill(dict(_index_=k, _value_=v))
                    calculated_values.append(calculate_valuer)
            elif isinstance(value, (list, types.GeneratorType)):
                for i in range(len(value)):
                    calculate_valuer = self.calculate_valuer.clone()
                    if isinstance(value[i], dict):
                        calculate_valuer.fill(dict(_index_=i, **value[i]))
                    else:
                        calculate_valuer.fill(dict(_index_=i, _value_=value[i]))
                    calculated_values.append(calculate_valuer)
            elif isinstance(value, range_type):
                for i in value:
                    calculate_valuer = self.calculate_valuer.clone()
                    calculate_valuer.fill(dict(_index_=i))
                    calculated_values.append(calculate_valuer)

        if self.calculate_wait_loaded:
            calculated_values, values = self.calculated_values, []
            for valuer in calculated_values:
                try:
                    values.append(self.do_filter(valuer.get()))
                except ContinueReturn as e:
                    if e.value != ContinueReturn.NULL:
                        values.append(e.value)
                    continue
                except BreakReturn as e:
                    if e.value != BreakReturn.NULL:
                        values.append(e.value)
                    break
            if self.return_valuer:
                return self.return_valuer.fill(values).get()
            return values

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


class ContextForeachValuer(ForeachValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        self.calculated_values_context_id = (id(self), "calculated_values")
        super(ContextForeachValuer, self).__init__(*args, **kwargs)

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
    def calculated_values(self):
        try:
            return self.contexter.values[self.calculated_values_context_id]
        except KeyError:
            return []

    @calculated_values.setter
    def calculated_values(self, v):
        if not v:
            if self.calculated_values_context_id in self.contexter.values:
                self.contexter.values.pop(self.calculated_values_context_id)
            return
        self.contexter.values[self.calculated_values_context_id] = v


class BreakValuer(Valuer):
    def __init__(self, return_valuer, inherit_valuers, *args, **kwargs):
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(BreakValuer, self).__init__(*args, **kwargs)

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self, contexter=None):
        return_valuer = self.return_valuer.clone(contexter) if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone(contexter) for inherit_valuer in self.inherit_valuers] \
            if self.inherit_valuers else None
        if contexter is not None:
            return ContextBreakValuer(return_valuer, inherit_valuers, self.key, self.filter, from_valuer=self,
                                      contexter=contexter)
        if isinstance(self, ContextBreakValuer):
            return ContextBreakValuer(return_valuer, inherit_valuers, self.key, self.filter, from_valuer=self,
                                      contexter=self.contexter)
        return self.__class__(return_valuer, inherit_valuers, self.key, self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            self.return_valuer.fill(data)
        return self

    def get(self):
        if self.return_valuer:
            raise BreakReturn(self.return_valuer.get())
        raise BreakReturn()

    def childs(self):
        childs = []
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        fields = []
        if self.return_valuer:
            for field in self.return_valuer.get_fields():
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


class ContextBreakValuer(BreakValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextBreakValuer, self).__init__(*args, **kwargs)

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


class ContinueValuer(Valuer):
    def __init__(self, return_valuer, inherit_valuers, *args, **kwargs):
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(ContinueValuer, self).__init__(*args, **kwargs)

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self, contexter=None):
        return_valuer = self.return_valuer.clone(contexter) if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone(contexter) for inherit_valuer in self.inherit_valuers] \
            if self.inherit_valuers else None
        if contexter is not None:
            return ContextContinueValuer(return_valuer, inherit_valuers, self.key, self.filter, from_valuer=self,
                                         contexter=contexter)
        if isinstance(self, ContextContinueValuer):
            return ContextContinueValuer(return_valuer, inherit_valuers, self.key, self.filter, from_valuer=self,
                                         contexter=self.contexter)
        return self.__class__(return_valuer, inherit_valuers, self.key, self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            self.return_valuer.fill(data)
        return self

    def get(self):
        if self.return_valuer:
            raise ContinueReturn(self.return_valuer.get())
        raise ContinueReturn()

    def childs(self):
        childs = []
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        fields = []
        if self.return_valuer:
            for field in self.return_valuer.get_fields():
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


class ContextContinueValuer(ContinueValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextContinueValuer, self).__init__(*args, **kwargs)

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
