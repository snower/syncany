# -*- coding: utf-8 -*-
# 2022/08/23
# create by: snower

class ConfigReader(object):
    def __init__(self, name):
        self.name = name

    def read(self):
        raise NotImplementedError