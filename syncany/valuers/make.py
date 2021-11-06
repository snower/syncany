# -*- coding: utf-8 -*-
# 2020/6/29
# create by: snower

from .valuer import Valuer


class MakeValuer(Valuer):
    def __init__(self, value_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.value_valuer = value_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(MakeValuer, self).__init__(*args, **kwargs)

    def init_valuer(self):
        self.wait_loaded = True if not self.return_valuer else False
        if not self.wait_loaded and self.return_valuer:
            self.check_wait_loaded()

    def check_wait_loaded(self):
        if isinstance(self.value_valuer, dict):
            for _, (key_valuer, value_valuer) in self.value_valuer.items():
                if key_valuer and key_valuer.require_loaded():
                    self.wait_loaded = True
                    return
                if value_valuer.require_loaded():
                    self.wait_loaded = True
                    return
        elif isinstance(self.value_valuer, list):
            for value_valuer in self.value_valuer:
                if value_valuer.require_loaded():
                    self.wait_loaded = True
                    return
        elif isinstance(self.value_valuer, Valuer):
            if self.value_valuer.require_loaded():
                self.wait_loaded = True
                return

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        if isinstance(self.value_valuer, dict):
            value_valuer = {key: (key_valuer.clone(), value_valuer.clone())
                            for key, (key_valuer, value_valuer) in self.value_valuer.items()}
        elif isinstance(self.value_valuer, list):
            value_valuer = [valuer.clone() for valuer in self.value_valuer]
        elif isinstance(self.value_valuer, Valuer):
            value_valuer = self.value_valuer.clone()
        else:
            value_valuer = None
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(value_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, wait_loaded=self.wait_loaded)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if isinstance(self.value_valuer, dict):
            for _, (key_valuer, value_valuer) in self.value_valuer.items():
                if key_valuer:
                    key_valuer.fill(data)
                value_valuer.fill(data)
        elif isinstance(self.value_valuer, list):
            for value_valuer in self.value_valuer:
                value_valuer.fill(data)
        elif isinstance(self.value_valuer, Valuer):
            self.value_valuer.fill(data)

        if self.return_valuer and not self.wait_loaded:
            if isinstance(self.value_valuer, dict):
                result = {}
                for key, (key_valuer, value_valuer) in self.value_valuer.items():
                    kv = key_valuer.get()
                    vv = value_valuer.get()
                    if isinstance(kv, list):
                        for ki in range(len(kv)):
                            result[kv[ki]] = vv[ki] if isinstance(vv, list) and len(vv) > ki else None
                    else:
                        result[kv] = vv
            elif isinstance(self.value_valuer, list):
                result = [value_valuer.get() for value_valuer in self.value_valuer]
                if len(self.value) == 1 and isinstance(self.value[0], list):
                    self.value = self.value[0]
            elif isinstance(self.value_valuer, Valuer):
                result = self.do_filter(self.value_valuer.get())
            else:
                result = None
            self.return_valuer.fill(result)
        return self

    def get(self):
        if not self.return_valuer or self.wait_loaded:
            if isinstance(self.value_valuer, dict):
                self.value = {}
                for key, (key_valuer, value_valuer) in self.value_valuer.items():
                    kv = key_valuer.get()
                    vv = value_valuer.get()
                    if isinstance(kv, list):
                        for ki in range(len(kv)):
                            self.value[kv[ki]] = vv[ki] if isinstance(vv, list) and len(vv) > ki else None
                    else:
                        self.value[kv] = vv
            elif isinstance(self.value_valuer, list):
                self.value = [value_valuer.get() for value_valuer in self.value_valuer]
                if len(self.value) == 1 and isinstance(self.value[0], list):
                    self.value = self.value[0]
            elif isinstance(self.value_valuer, Valuer):
                self.do_filter(self.value_valuer.get())
            else:
                self.value = None
            if self.return_valuer:
                self.return_valuer.fill(self.value)
        if self.return_valuer:
            return self.return_valuer.get()
        return self.value

    def childs(self):
        childs = []
        if isinstance(self.value_valuer, dict):
            for _, (key_valuer, value_valuer) in self.value_valuer.items():
                childs.append(key_valuer)
                childs.append(value_valuer)
        elif isinstance(self.value_valuer, list):
            for value_valuer in self.value_valuer:
                childs.append(value_valuer)
        elif isinstance(self.value_valuer, Valuer):
            childs.append(self.value_valuer)
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            childs.extend(self.inherit_valuers)
        return []

    def get_fields(self):
        fields = []
        if isinstance(self.value_valuer, dict):
            for _, (key_valuer, value_valuer) in self.value_valuer.items():
                for field in key_valuer.get_fields():
                    fields.append(field)
                for field in value_valuer.get_fields():
                    fields.append(field)
        elif isinstance(self.value_valuer, list):
            for value_valuer in self.value_valuer:
                for field in value_valuer.get_fields():
                    fields.append(field)
        elif isinstance(self.value_valuer, Valuer):
            for field in self.value_valuer.get_fields():
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

        if isinstance(self.value_valuer, Valuer):
            return self.value_valuer.get_final_filter()

        return None