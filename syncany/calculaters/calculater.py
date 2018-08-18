# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

class Calculater(object):
    def __init__(self, *args):
        self.args = args

    def calculate(self):
        if not self.args:
            return None

        if len(self.args) == 1:
            return self.args[0]

        return self.args