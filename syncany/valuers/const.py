# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .valuer import Valuer

class ConstValuer(Valuer):
    def __init__(self, value, *args, **kwargs):
        super(ConstValuer, self).__init__(*args, **kwargs)

        self.value = value

    def clone(self):
        return self.__class__(self.value, self.key, self.filter)

    def fill(self, data):
        return self