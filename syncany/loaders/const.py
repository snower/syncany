# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .loader import Loader

class ConstLoader(Loader):
    def __init__(self, datas, *args, **kwargs):
        super(ConstLoader, self).__init__(*args, **kwargs)

        self.const_datas = datas

    def load(self):
        if self.loaded:
            return

        for data in self.const_datas:
            primary_key = self.get_data_primary_key(data)

            values = {}
            for key, field in self.schema.items():
                values[key] = field.clone().fill(data)

            self.data_keys[primary_key] = values
            self.datas.append(values)
        self.loaded = True