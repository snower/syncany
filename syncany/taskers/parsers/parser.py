# -*- coding: utf-8 -*-
# 2021/10/29
# create by: snower

class Parser(object):
    def __init__(self, filename):
        self.filename = filename

    def load(self):
        raise NotImplementedError