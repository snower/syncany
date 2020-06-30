# -*- coding: utf-8 -*-
# 2020/6/29
# create by: snower


from .valuer import Valuer


class MakeValuer(Valuer):
    def __init__(self, value_valuer, loop, loop_valuer, condition, condition_valuer, condition_break, return_valuer,
                 inherit_valuers, *args, **kwargs):
        super(MakeValuer, self).__init__(*args, **kwargs)

        self.value_valuer = value_valuer
        self.loop = loop
        self.loop_valuer = loop_valuer
        self.condition = condition
        self.condition_valuer = condition_valuer
        self.condition_break = condition_break
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.wait_loaded = True if not self.return_valuer else False
        self.loop_wait_loaded = True if self.loop_valuer and self.loop_valuer.require_loaded() else False
        self.condition_wait_loaded = True if self.condition_valuer and self.condition_valuer.require_loaded() else False
        self.loop_result_valuers = None

        if self.return_valuer:
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
        elif isinstance(self.value_valuer, (list, tuple, set)):
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
        elif isinstance(self.value_valuer, (list, tuple, set)):
            value_valuer = [valuer.clone() for valuer in self.value_valuer]
        elif isinstance(self.value_valuer, Valuer):
            value_valuer = self.value_valuer.clone()
        else:
            value_valuer = None
        loop_valuer = self.loop_valuer.clone() if self.loop_valuer else None
        condition_valuer = self.condition_valuer.clone() if self.condition_valuer else None
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(value_valuer, self.loop, loop_valuer, self.condition, condition_valuer,
                              self.condition_break, return_valuer, inherit_valuers, self.key, self.filter)

    def fill_for_item(self, data):
        if isinstance(self.value_valuer, dict):
            value_valuer = {key: (key_valuer.clone(), value_valuer.clone())
                            for key, (key_valuer, value_valuer) in self.value_valuer.items()}
        elif isinstance(self.value_valuer, (list, tuple, set)):
            value_valuer = [valuer.clone() for valuer in self.value_valuer]
        elif isinstance(self.value_valuer, Valuer):
            value_valuer = self.value_valuer.clone()
        else:
            value_valuer = None
        condition_valuer = self.condition_valuer.clone() if self.condition_valuer else None
        value_valuer = self.__class__(value_valuer, None, None, self.condition, condition_valuer,
                                      None, None, None, self.key, self.filter)
        value_valuer.fill(data)
        return value_valuer

    def fill_for(self, data):
        if isinstance(data, (list, tuple, set)):
            self.loop_result_valuers = []
            for d in data:
                value_valuer = self.fill_for_item(d)
                if self.condition_valuer and not self.condition_wait_loaded:
                    if value_valuer.condition_valuer.get():
                        self.loop_result_valuers.append(value_valuer)
                        if self.condition_break == "break":
                            break
                else:
                    self.loop_result_valuers.append(value_valuer)
        else:
            value_valuer = self.fill_for_item(data)
            if self.condition_valuer and not self.condition_wait_loaded:
                if value_valuer.condition_valuer.get():
                    self.loop_result_valuers = [value_valuer]
                else:
                    self.loop_result_valuers = []
            else:
                self.loop_result_valuers = [value_valuer]

        if self.return_valuer and not self.wait_loaded:
            result = []
            for value_valuer in self.loop_result_valuers:
                v = value_valuer.get()
                if self.condition_valuer and self.condition_wait_loaded:
                    if v is not None:
                        result.append(v)
                        if self.condition_break == "break":
                            break
                else:
                    result.append(v)
            self.return_valuer.fill(result)
        return self

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.loop == "#for":
            if self.loop_valuer:
                self.loop_valuer.fill(data)
                if self.loop_wait_loaded:
                    return self
                data = self.loop_valuer.get()
            return self.fill_for(data)

        if self.condition == "#if":
            if self.condition_valuer:
                self.condition_valuer.fill(data)

        if isinstance(self.value_valuer, dict):
            for _, (key_valuer, value_valuer) in self.value_valuer.items():
                if key_valuer:
                    key_valuer.fill(data)
                value_valuer.fill(data)
        elif isinstance(self.value_valuer, (list, tuple, set)):
            for value_valuer in self.value_valuer:
                value_valuer.fill(data)
        elif isinstance(self.value_valuer, Valuer):
            self.value_valuer.fill(data)

        if self.condition == "#if":
            if self.condition_valuer:
                if self.condition_wait_loaded:
                    return self
                if not self.condition_valuer.get():
                    return self

        if self.return_valuer and not self.wait_loaded:
            if isinstance(self.value_valuer, dict):
                result = {key_valuer.get(): value_valuer.get() for key, (key_valuer, value_valuer) in self.value_valuer.items()}
            elif isinstance(self.value_valuer, (list, tuple, set)):
                result = [value_valuer.get() for value_valuer in self.value_valuer]
            elif isinstance(self.value_valuer, Valuer):
                result = self.value_valuer.get()
            else:
                result = None
            self.return_valuer.fill(result)
        return self

    def get(self):
        if self.loop == "#for":
            if self.loop_valuer:
                if self.loop_wait_loaded:
                    data = self.loop_valuer.get()
                    self.fill_for(data)
            if self.return_valuer and not self.wait_loaded:
                return self.return_valuer.get()

            self.value = []
            for value_valuer in self.loop_result_valuers:
                v = value_valuer.get()
                if self.condition_valuer and self.condition_wait_loaded:
                    if v is not None:
                        self.value.append(v)
                        if self.condition_break == "break":
                            break
                else:
                    self.value.append(v)
            if self.condition_break == "break":
                if not self.value:
                    return None
                if len(self.value) == 1:
                    return self.value[0]
            return self.value

        if self.condition == "#if":
            if self.condition_valuer and self.condition_wait_loaded:
                if not self.condition_valuer.get():
                    return None
            else:
                if self.return_valuer and not self.wait_loaded:
                    return self.return_valuer.get()
        else:
            if self.return_valuer and not self.wait_loaded:
                return self.return_valuer.get()

        if isinstance(self.value_valuer, dict):
            self.value = {key_valuer.get(): value_valuer.get() for key, (key_valuer, value_valuer) in self.value_valuer.items()}
        elif isinstance(self.value_valuer, (list, tuple, set)):
            self.value = [value_valuer.get() for value_valuer in self.value_valuer]
        elif isinstance(self.value_valuer, Valuer):
            self.value = self.value_valuer.get()
        else:
            self.value = None
        return self.value

    def childs(self):
        childs = []
        if isinstance(self.value_valuer, dict):
            for _, (key_valuer, value_valuer) in self.value_valuer.items():
                childs.append(key_valuer)
                childs.append(value_valuer)
        elif isinstance(self.value_valuer, (list, tuple, set)):
            for value_valuer in self.value_valuer:
                childs.append(value_valuer)
        elif isinstance(self.value_valuer, Valuer):
            childs.append(self.value_valuer)
        if self.loop_valuer:
            childs.append(self.loop_valuer)
        if self.condition_valuer:
            childs.append(self.condition_valuer)
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
        elif isinstance(self.value_valuer, (list, tuple, set)):
            for value_valuer in self.value_valuer:
                for field in value_valuer.get_fields():
                    fields.append(field)
        elif isinstance(self.value_valuer, Valuer):
            for field in self.value_valuer.get_fields():
                fields.append(field)
        if self.loop_valuer:
            for field in self.loop_valuer.get_fields():
                fields.append(field)
        if self.condition_valuer:
            for field in self.condition_valuer.get_fields():
                fields.append(field)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        if self.filter:
            return self.filter

        if self.return_valuer:
            return self.return_valuer.get_final_filter()

        if isinstance(self.value_valuer, Valuer):
            return self.value_valuer.get_final_filter()

        return None