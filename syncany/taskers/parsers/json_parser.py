# -*- coding: utf-8 -*-
# 2021/10/29
# create by: snower

import json
from .parser import Parser

class JsonParser(Parser):
    def load(self):
        with open(self.filename, "r") as fp:
            config = json.load(fp)
        return config