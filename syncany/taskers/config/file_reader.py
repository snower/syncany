# -*- coding: utf-8 -*-
# 2022/08/23
# create by: snower

import os
from .reader import ConfigReader

class FileConfigReader(ConfigReader):
    def read(self):
        with open(self.name, "r", encoding=os.environ.get("SYNCANYENCODING", "utf-8")) as fp:
            filename_infos = self.name.split(".")
            return (filename_infos[-1].lower() if filename_infos else "json"), fp.read()
