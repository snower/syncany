# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import datetime
from ..utils import ensure_timezone
from .valuer import Valuer


class ConstValuer(Valuer):
    def __init__(self, value, *args, **kwargs):
        self.value = value
        super(ConstValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(ConstValuer, self).new_init()
        self.value = self.do_filter(self.value)

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

    def do_filter(self, value):
        if not self.filter:
            if isinstance(value, datetime.datetime):
                value = ensure_timezone(value)
            return value
        return self.filter.filter(value)

    def require_loaded(self):
        return False

    def is_const(self):
        return True

    def is_aggregate(self):
        return False

    def is_yield(self):
        return False
