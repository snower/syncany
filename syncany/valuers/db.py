# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .valuer import Valuer

class DBValuer(Valuer):
    def get_fields(self):
        return [self.key]