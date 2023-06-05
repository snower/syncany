# -*- coding: utf-8 -*-
# 18/8/9
# create by: snower

class Filter(object):
    _default_instance = None

    @classmethod
    def default(cls):
        if cls._default_instance is None:
            cls._default_instance = cls()
        return cls._default_instance

    def __init__(self, args=None):
        self.args = args

    def filter(self, value):
        return value

    def sprintf(self, value):
        return str(value)

    def __call__(self, value):
        return self.filter(value)