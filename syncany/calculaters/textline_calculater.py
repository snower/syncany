# -*- coding: utf-8 -*-
# 2023/2/21
# create by: snower

from .calculater import Calculater


class TextLineSplitCalculater(Calculater):
    ESCAPE_CHARS = ['\a', '\b', '\f', '\n', '\r', '\t', '\v', '\\', '\'', '"', '\0']

    def __init__(self, *args, **kwargs):
        super(TextLineSplitCalculater, self).__init__(*args, **kwargs)

        self.line_text = ""
        self.index = 0
        self.len = 0

    def next(self):
        self.index += 1

    def skip_escape(self, c):
        start_index = self.index
        self.next()
        while self.index < self.len:
            if self.line_text[self.index] != c:
                self.next()
                continue
            backslash = self.index - 1
            while backslash >= 0:
                if self.line_text[backslash] != '\\':
                    break
                backslash -= 1
            if (self.index - backslash + 1) % 2 != 0:
                self.next()
                continue
            self.next()
            return start_index, self.index, self.line_text[start_index: self.index]
        self.next()
        raise EOFError(self.line_text[start_index:])

    def read_util(self, cs, escape_chars=('"', "'")):
        start_index = self.index
        while self.index < self.len:
            if self.line_text[self.index] in escape_chars:
                self.skip_escape(self.line_text[self.index])
                continue
            if self.line_text[self.index: self.index + len(cs)] != cs:
                self.next()
                continue
            return start_index, self.index + len(cs) - 1, self.line_text[start_index: self.index + len(cs) - 1]
        raise EOFError(self.line_text[start_index:])

    def split(self, line_text, split=' '):
        self.line_text = line_text.strip()
        self.index = 0
        self.len = len(self.line_text)

        fields, field_index = {}, 0
        start_index = self.index
        try:
            while self.index < self.len:
                if self.line_text[self.index] in ('"', "'"):
                    self.skip_escape(self.line_text[self.index])
                    continue
                if self.line_text[self.index] in ("[", "("):
                    self.read_util(']' if self.line_text[self.index] == '[' else ')')
                    self.next()
                    continue
                if self.line_text[self.index] == split:
                    fields["field%d" % field_index] = self.line_text[start_index: self.index]
                    field_index += 1
                    self.next()
                    start_index = self.index
                    continue
                self.next()
        except EOFError:
            pass
        if start_index < self.index:
            fields["field%d" % field_index] = self.line_text[start_index: self.index]
        return fields

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
            for data in self.args[0]:
                if self.args[1] not in data:
                    continue
                line = str(data[self.args[1]])
                data = self.split(line, self.args[2] if len(self.args) >= 3 and isinstance(self.args[2], str) else ' ')
                if len(keys) < len(data):
                    for i in range(len(keys), len(data)):
                        keys.append("field%d" % i)
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
            line = str(self.args[0][self.args[1]])
            data = self.split(line, self.args[2] if len(self.args) >= 3 and isinstance(self.args[2], str) else ' ')
            data["line"] = line
            return data

        if isinstance(self.args[0], str):
            data = self.split(self.args[0], self.args[1] if len(self.args) >= 2 and isinstance(self.args[1], str) else ' ')
            data["line"] = self.args[0]
            return data
        return None


class TextLineCalculater(Calculater):
    def calculate(self):
        if self.name[10:] == "split":
            calculater = TextLineSplitCalculater(self.name[10:], *self.args)
            return calculater.calculate()
        return None