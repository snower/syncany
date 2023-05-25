# -*- coding: utf-8 -*-
# 2020/7/3
# create by: snower

from .valuer import Valuer, LoadAllFieldsException


class CallReturnManager(object):
    def __init__(self):
        self.datas = {}

    def loaded(self, key):
        if not isinstance(key, (int, float, str, bytes)):
            key = "@id_%s" % id(key)
        if key not in self.datas:
            return key, False
        return key, True

    def get(self, key):
        if key not in self.datas:
            return None
        return self.datas[key]

    def set(self, key, value):
        self.datas[key] = value

    def reset(self):
        self.datas.clear()


class CallValuer(Valuer):
    calculated = False

    def __init__(self, value_valuer, calculate_valuer, return_valuer, inherit_valuers, return_manager, *args, **kwargs):
        self.value_valuer = value_valuer
        self.calculate_valuer = calculate_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.return_manager = return_manager or CallReturnManager()
        super(CallValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(CallValuer, self).new_init()
        self.value_wait_loaded = False if not self.value_valuer else self.value_valuer.require_loaded()
        self.calculate_wait_loaded = True if self.calculate_valuer and self.calculate_valuer.require_loaded() else False
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False

    def clone_init(self, from_valuer):
        super(CallValuer, self).clone_init(from_valuer)
        self.value_wait_loaded = from_valuer.value_wait_loaded
        self.calculate_wait_loaded = from_valuer.calculate_wait_loaded
        self.wait_loaded = from_valuer.wait_loaded

    def get_manager(self):
        return self.return_manager

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_loader(self, is_return_getter=True, **kwargs):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_loader(is_return_getter=False, **kwargs)
        self.value_valuer.mount_loader(is_return_getter=False, **kwargs)
        if self.calculate_valuer:
            self.calculate_valuer.mount_loader(is_return_getter=False, **kwargs)
        if self.return_valuer:
            self.return_valuer.mount_loader(is_return_getter=is_return_getter and True, **kwargs)

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        value_valuer = self.value_valuer.clone(contexter, **kwargs)
        calculate_valuer = self.calculate_valuer.clone(contexter, **kwargs) if self.calculate_valuer else None
        return_valuer = self.return_valuer.clone(contexter, **kwargs) if self.return_valuer else None
        if contexter is not None:
            return ContextCallValuer(value_valuer, calculate_valuer, return_valuer, inherit_valuers,
                                     self.return_manager, self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextCallValuer):
            return ContextCallValuer(value_valuer, calculate_valuer, return_valuer, inherit_valuers,
                                     self.return_manager, self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(value_valuer, calculate_valuer, return_valuer, inherit_valuers,
                              self.return_manager, self.key, self.filter, from_valuer=self)

    def reinit(self):
        self.calculated = False
        return super(CallValuer, self).reinit()

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.value_valuer:
            self.value_valuer.fill(data)
            if not self.value_wait_loaded:
                value = self.value_valuer.get()
            else:
                value = None
        else:
            value = data

        if not self.value_wait_loaded:
            calculated_key, calculated = self.return_manager.loaded(value)
            if not calculated:
                if not self.calculate_wait_loaded:
                    value = self.do_filter(self.calculate_valuer.fill_get(value))
                    self.return_manager.set(calculated_key, value)
                    if self.return_valuer:
                        if not self.wait_loaded:
                            self.value = self.return_valuer.fill_get(value)
                        else:
                            self.return_valuer.fill(value)
                    else:
                        self.value = value
                    self.calculated = True
                else:
                    self.calculate_valuer.fill(value)
                    self.value = value
            else:
                value = self.return_manager.get(calculated_key)
                if self.return_valuer:
                    if not self.wait_loaded:
                        self.value = self.return_valuer.fill_get(value)
                    else:
                        self.return_valuer.fill(value)
                else:
                    self.value = value
                self.calculated = True
        return self

    def get(self):
        if self.calculated:
            if self.return_valuer:
                if not self.wait_loaded:
                    return self.value
                return self.return_valuer.get()
            return self.value

        if self.value_wait_loaded:
            value = self.value_valuer.get()
            calculated_key, calculated = self.return_manager.loaded(value)
            if not calculated:
                value = self.do_filter(self.calculate_valuer.fill_get(value))
                self.return_manager.set(calculated_key, value)
            else:
                value = self.return_manager.get(calculated_key)
            if self.return_valuer:
                return self.return_valuer.fill_get(value)
            return value

        if self.calculate_wait_loaded:
            calculated_key, calculated = self.return_manager.loaded(self.value)
            if not calculated:
                value = self.do_filter(self.calculate_valuer.get())
                self.return_manager.set(calculated_key, value)
            else:
                value = self.return_manager.get(calculated_key)
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

        value = self.value_valuer.fill_get(data) if self.value_valuer else data
        calculated_key, calculated = self.return_manager.loaded(value)
        if not calculated:
            value = self.do_filter(self.calculate_valuer.fill_get(value))
            self.return_manager.set(calculated_key, value)
            if self.return_valuer:
                return self.return_valuer.fill_get(value)
            return value
        value = self.return_manager.get(calculated_key)
        if self.return_valuer:
            return self.return_valuer.fill_get(value)
        return value

    def reset(self):
        self.return_manager.reset()
        super(CallValuer, self).reset()

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

        if self.filter:
            return self.filter

        if self.calculate_valuer:
            return self.calculate_valuer.get_final_filter()

        return None


class ContextCallValuer(CallValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        self.calculated_context_id = (id(self), "calculated")
        super(ContextCallValuer, self).__init__(*args, **kwargs)

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
    def calculated(self):
        try:
            return self.contexter.values[self.calculated_context_id]
        except KeyError:
            return False

    @calculated.setter
    def calculated(self, v):
        if v is False:
            if self.calculated_context_id in self.contexter.values:
                self.contexter.values.pop(self.calculated_context_id)
            return
        self.contexter.values[self.calculated_context_id] = v
