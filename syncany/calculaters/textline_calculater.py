# -*- coding: utf-8 -*-
# 2023/2/21
# create by: snower

from ..taskers.context import TaskerContext
from .calculater import Calculater


class TextLineSplitCalculater(Calculater):
    def update_outputer_schema(self, xkeys):
        from ..taskers.tasker import current_tasker
        tasker = current_tasker()
        tasker.outputer.schema = {}
        for key in xkeys:
            valuer = tasker.create_valuer(tasker.valuer_compiler.compile_data_valuer(key, None))
            if not valuer:
                continue
            tasker.outputer.add_valuer(key, valuer)

    def calculate(self):
        if not self.args:
            return None

        if len(self.args) >= 2 and isinstance(self.args[0], list):
            keys, datas = [], []
            from ..database.textline import TextLineSpliter
            textline_spliter = TextLineSpliter(self.args[2] if len(self.args) >= 3 and isinstance(self.args[2], str) else ' ')
            for data in self.args[0]:
                if self.args[1] not in data:
                    continue
                line = str(data[self.args[1]])
                data = textline_spliter.split(line)
                if len(keys) < len(data):
                    for i in range(len(keys), len(data)):
                        keys.append("seg%d" % i)
                data["line"] = line
                datas.append(data)
            self.update_outputer_schema(keys)
            for data in datas:
                for key in keys:
                    if key not in data:
                        data[key] = None
            return datas

        if len(self.args) >= 2 and isinstance(self.args[0], dict):
            if self.args[1] not in self.args[0]:
                return None
            cache = TaskerContext.current().cache("TextLineSplitCalculater::values")
            cache_value = cache.get(id(self.args[0][self.args[1]]))
            if cache_value is not None:
                return cache_value
            from ..database.textline import TextLineSpliter
            textline_spliter = TextLineSpliter(self.args[2] if len(self.args) >= 3 and isinstance(self.args[2], str) else ' ')
            line = str(self.args[0][self.args[1]])
            data = textline_spliter.split(line)
            data["line"] = line
            cache.set(id(self.args[0][self.args[1]]), data)
            return data

        if isinstance(self.args[0], str):
            cache = TaskerContext.current().cache("TextLineSplitCalculater::values")
            cache_value = cache.get(id(self.args[0]))
            if cache_value is not None:
                return cache_value
            from ..database.textline import TextLineSpliter
            textline_spliter = TextLineSpliter(self.args[1] if len(self.args) >= 2 and isinstance(self.args[1], str) else ' ')
            data = textline_spliter.split(self.args[0])
            data["line"] = self.args[0]
            cache.set(id(self.args[0]), data)
            return data
        return None


class TextLineCalculater(Calculater):
    def calculate(self):
        if self.name[10:] == "split":
            calculater = TextLineSplitCalculater(self.name[10:], *self.args)
            return calculater.calculate()
        return None