# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .valuer import Valuer


class ConstValuer(Valuer):
    def __init__(self, value, *args, **kwargs):
        super(ConstValuer, self).__init__(*args, **kwargs)

        self.value = self.do_filter(value)

    def mount_loader(self, is_return_getter=False, **kwargs):
        pass

    def clone(self, contexter=None, **kwargs):
        return self.__class__(self.value, self.key, self.filter, from_valuer=self)

    def reinit(self):
        return self

    def fill(self, data):
        return self

    def fill_get(self, data):
        return self.value

    def require_loaded(self):
        return False
