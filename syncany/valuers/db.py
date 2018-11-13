# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from .valuer import Valuer

class DBValuer(Valuer):
    def get_fields(self):
        if not self.key or self.key == "*":
            return []

        keys = [key for key in self.key.split(".") if key and key[0] != ":"]
        if not keys:
            return []

        return keys