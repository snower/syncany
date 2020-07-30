# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

from collections import OrderedDict

class Tasker(object):
    name = ""

    def __init__(self):
        self.arguments = {}
        self.input = ""
        self.output = ""
        self.databases = {}
        self.schema = OrderedDict()
        self.loader = None
        self.outputer = None

    def get_loader(self):
        pass

    def load(self):
        pass

    def compile(self, arguments):
        self.arguments = arguments

    def run(self):
        pass