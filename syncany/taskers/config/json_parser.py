# -*- coding: utf-8 -*-
# 2021/10/29
# create by: snower

import json
from .parser import Parser


class JsonParser(Parser):
    def parse(self):
        return json.loads(self.content)