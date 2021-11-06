# -*- coding: utf-8 -*-
# 2021/10/29
# create by: snower

class Parser(object):
    def __init__(self, content):
        self.content = content

    def parse(self):
        raise NotImplementedError