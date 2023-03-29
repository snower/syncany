# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .valuer import Valuer

class ConstValuer(Valuer):
    def __init__(self, value, *args, **kwargs):
        super(ConstValuer, self).__init__(*args, **kwargs)

        self.value = value

    def clone(self):
        return self.__class__(self.value, self.key, self.filter, from_valuer=self)

    def reinit(self):
        return self

    def fill(self, data):
        self.do_filter(self.value)
        return self

    def require_loaded(self):
        return False