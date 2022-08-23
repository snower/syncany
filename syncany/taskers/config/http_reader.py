# -*- coding: utf-8 -*-
# 2022/08/23
# create by: snower

from .reader import ConfigReader

class HttpConfigReader(ConfigReader):
    _requests = None

    def init(self):
        try:
            import requests
        except:
            raise ImportError("requests>=2.22.0 is required")
        self.__class__._requests = requests

    def read(self):
        if self._requests is None:
            self.init()

        res = self._requests.get(self.name)
        content_type = res.headers.get("Content-Type")
        if not content_type:
            url_infos = self.name.split("?")[0].split(".") if "?" in self.name else self.name.split(".")
            return (url_infos[-1].lower() if url_infos else "json"), res.text
        if "json" in content_type:
            return "json", res.text
        if "yaml" in content_type:
            return "yaml", res.text
        return content_type, res.text
