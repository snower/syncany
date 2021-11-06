# -*- coding: utf-8 -*-
# 2021/11/6
# create by: snower

class DataValuerLoaderOption:
    pass


class DataValuerOutputerOption:
    def __init__(self, changed_require_update=False):
        self.changed_require_update = changed_require_update
