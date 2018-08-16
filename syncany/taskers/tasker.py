# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

import argparse
from collections import OrderedDict

class Tasker(object):
    name = ""

    def __init__(self):
        self.argparse = argparse.ArgumentParser(description='syncany %s' % self.name)
        self.arguments = {}
        self.input = ""
        self.output = ""
        self.databases = {}
        self.schema = OrderedDict()
        self.loader = None
        self.outputer = None

    def get_loader(self):
        pass

    def add_argument(self, *args, **kwargs):
        self.argparse.add_argument(*args, **kwargs)

    def run(self):
        self.arguments = self.argparse.parse_args()