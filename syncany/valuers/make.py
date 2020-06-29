# -*- coding: utf-8 -*-
# 2020/6/29
# create by: snower


from .valuer import Valuer

class MakeValuer(Valuer):
    def __init__(self, value_valuer, loop, *args, **kwargs):
        super(MakeValuer, self).__init__(*args, **kwargs)

        self.value_valuer = value_valuer
        self.loop = loop
        self.result_valuer = None

    def clone(self):
        if isinstance(self.value_valuer, dict):
            value_valuer = {key: valuer.clone() for key, valuer in self.value_valuer.items()}
        elif isinstance(self.value_valuer, (list, tuple, set)):
            value_valuer = [valuer.clone() for valuer in self.value_valuer]
        else:
            value_valuer = None
        valuer = self.__class__(value_valuer, self.loop, self.key, self.filter)
        valuer.result_valuer = self.result_valuer
        return valuer

    def fill_for_item(self, data):
        if isinstance(self.value_valuer, dict):
            result = {}
            for key, valuer in self.value_valuer.items():
                valuer = valuer.clone()
                valuer.fill(data)
                result[key] = valuer
            return result

        if isinstance(self.value_valuer, (list, tuple, set)):
            result = []
            for valuer in self.value_valuer:
                valuer = valuer.clone()
                valuer.fill(data)
                result.append(valuer)
            return result
        return None

    def fill_for(self, data):
        self.result_valuer = []
        if isinstance(data, (list, tuple, set)):
            for d in data:
                self.result_valuer.append(self.fill_for_item(d))
        else:
            self.result_valuer.append(self.fill_for_item(data))
        return self

    def fill(self, data):
        super(MakeValuer, self).fill(data)
        if self.loop == "#for":
            return self.fill_for(data)

        if isinstance(self.value_valuer, dict):
            for _, valuer in self.value_valuer.items():
                valuer.fill(data)
        elif isinstance(self.value_valuer, (list, tuple, set)):
            for valuer in self.value_valuer:
                valuer.fill(data)
        return self

    def get(self):
        if self.result_valuer is not None:
            self.value = []
            for v in self.result_valuer:
                if isinstance(v, dict):
                    self.value.append({key: valuer.get() for key, valuer in v.items()})
                elif isinstance(v, (list, tuple, set)):
                    self.value.append([valuer.get() for valuer in v])
            return self.value

        if isinstance(self.value_valuer, dict):
            self.value = {key: valuer.get() for key, valuer in self.value_valuer.items()}
        elif isinstance(self.value_valuer, (list, tuple, set)):
            self.value = [valuer.get() for valuer in self.value_valuer]
        else:
            self.value = None
        return self.value

    def childs(self):
        if isinstance(self.value_valuer, dict):
            return [valuer for _, valuer in self.value_valuer.items()]
        elif isinstance(self.value_valuer, (list, tuple, set)):
            return [valuer for valuer in self.value_valuer]
        return []

    def get_fields(self):
        fields = []
        if isinstance(self.value_valuer, dict):
            for _, valuer in self.value_valuer.items():
                for field in valuer.get_fields():
                    fields.append(field)
        elif isinstance(self.value_valuer, (list, tuple, set)):
            for valuer in self.value_valuer:
                for field in valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        return None