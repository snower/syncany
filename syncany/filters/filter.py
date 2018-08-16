# -*- coding: utf-8 -*-
# 18/8/9
# create by: snower

class Filter(object):
    def __init__(self, args = None):
        self.args = args

    def filter(self, value):
        return value

    def __call__(self, value):
        return self.filter(value)