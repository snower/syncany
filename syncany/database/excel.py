# -*- coding: utf-8 -*-
# 18/8/13
# create by: snower

import os
try:
    import openpyxl
except ImportError:
    openpyxl = None
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase

class ExeclFileNotFound(Exception):
    pass

class ExeclQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(ExeclQueryBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        self.query[(key, '>')] = (value, lambda a, b: a > b)

    def filter_gte(self, key, value):
        self.query[(key, ">=")] = (value, lambda a, b: a >= b)

    def filter_lt(self, key, value):
        self.query[(key, "<")] = (value, lambda a, b: a < b)

    def filter_lte(self, key, value):
        self.query[(key, "<=")] = (value, lambda a, b: a <= b)

    def filter_eq(self, key, value):
        self.query[(key, "==")] = (value, lambda a, b: a == b)

    def filter_ne(self, key, value):
        self.query[(key, "!=")] = (value, lambda a, b: a != b)

    def filter_in(self, key, value):
        self.query[(key, "in")] = (value, lambda a, b: a in b)

    def order_by(self, key, direct=1):
        self.orders.append((key, direct))

    def commit(self):
        execl_sheet = self.db.ensure_open_file(self.name)
        datas = []
        for data in execl_sheet.sheet_datas:
            succed = True
            for (key, exp), (value, cmp) in self.query.items():
                if key not in data:
                    succed = False
                    break
                if not cmp(data[key], value):
                    succed = False
                    break

            if succed:
                datas.append(data)

        if self.orders:
            datas = sorted(datas, key =  self.orders[0][0], reverse = True if self.orders[0][1] < 0 else False)

        return datas

class ExeclInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(ExeclInsertBuilder, self).__init__(*args, **kwargs)

        if isinstance(self.datas, dict):
            self.datas = [self.datas]

    def commit(self):
        execl_sheet = self.db.ensure_open_file(self.name)
        execl_sheet.sheet_datas.extend(self.datas)

class ExeclUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(ExeclUpdateBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        self.query[(key, '>')] = (value, lambda a, b: a > b)

    def filter_gte(self, key, value):
        self.query[(key, ">=")] = (value, lambda a, b: a >= b)

    def filter_lt(self, key, value):
        self.query[(key, "<")] = (value, lambda a, b: a < b)

    def filter_lte(self, key, value):
        self.query[(key, "<=")] = (value, lambda a, b: a <= b)

    def filter_eq(self, key, value):
        self.query[(key, "==")] = (value, lambda a, b: a == b)

    def filter_ne(self, key, value):
        self.query[(key, "!=")] = (value, lambda a, b: a != b)

    def filter_in(self, key, value):
        self.query[(key, "in")] = (value, lambda a, b: a in b)

    def commit(self):
        execl_sheet = self.db.ensure_open_file(self.name)
        datas = []
        for data in execl_sheet.sheet_datas:
            succed = True
            for (key, exp), (value, cmp) in self.query.items():
                if key not in data:
                    succed = False
                    break
                if not cmp(data[key], value):
                    succed = False
                    break

            if succed:
                datas.append(self.update)
            else:
                datas.append(data)

        execl_sheet.sheet_datas = datas
        return datas

class ExeclDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(ExeclDeleteBuilder, self).__init__(*args, **kwargs)

    def filter_gt(self, key, value):
        self.query[(key, '>')] = (value, lambda a, b: a > b)

    def filter_gte(self, key, value):
        self.query[(key, ">=")] = (value, lambda a, b: a >= b)

    def filter_lt(self, key, value):
        self.query[(key, "<")] = (value, lambda a, b: a < b)

    def filter_lte(self, key, value):
        self.query[(key, "<=")] = (value, lambda a, b: a <= b)

    def filter_eq(self, key, value):
        self.query[(key, "==")] = (value, lambda a, b: a == b)

    def filter_ne(self, key, value):
        self.query[(key, "!=")] = (value, lambda a, b: a != b)

    def filter_in(self, key, value):
        self.query[(key, "in")] = (value, lambda a, b: a in b)

    def commit(self):
        execl_sheet = self.db.ensure_open_file(self.name)
        datas = []
        for data in execl_sheet.sheet_datas:
            succed = True
            for (key, exp), (value, cmp) in self.query.items():
                if key not in data:
                    succed = False
                    break
                if not cmp(data[key], value):
                    succed = False
                    break

            if not succed:
                datas.append(data)

        execl_sheet.sheet_datas = datas
        return datas

class ExeclSheet(object):
    def __init__(self, name, filename, sheet_name, execl_fp, execl_sheet):
        self.name = name
        self.filename = filename
        self.sheet_name = sheet_name

        self.execl_fp = execl_fp
        self.execl_sheet = execl_sheet
        self.sheet_descriptions = []
        self.sheet_datas = []

    def load(self):
        for row in self.execl_sheet.rows:
            if not self.sheet_descriptions:
                for cel in row:
                    self.sheet_descriptions.append(cel.value)
            else:
                data, index = {}, 0
                for cel in row:
                    data[self.sheet_descriptions[index]] = cel.value
                    index += 1
                self.sheet_datas.append(data)

    def get_fields(self):
        fields = None
        for data in self.sheet_datas:
            if fields is None:
                fields = set(data.keys())
            else:
                fields = fields & set(data.keys())
        return tuple(fields) if fields else tuple()

    def close(self):
        if self.execl_sheet:
            sheet_index = self.execl_fp.index(self.execl_sheet)
            self.execl_fp.remove(self.execl_sheet)
            self.execl_sheet = self.execl_fp.create_sheet(self.sheet_name, sheet_index)
            fields = self.get_fields()
            for row_index in range(len(self.sheet_datas)):
                row = self.sheet_datas[row_index]
                for col_index in range(len(fields)):
                    col = row[fields[col_index]]
                    if isinstance(col, str):
                        col = col.encode("utf-8")
                    self.execl_sheet.cell(row_index + 1, col_index + 1, col)

class ExeclDB(DataBase):
    DEFAULT_CONFIG = {
        "path": "./",
    }

    def __init__(self, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        all_config["path"] = os.path.abspath(all_config["path"])

        super(ExeclDB, self).__init__(all_config)

        self.execls = {}
        self.execl_fps = {}

    def parse_filename(self, name):
        filename, sheet_name = None, None
        names = name.split(".")
        if len(names) >= 2:
            filenames = ".".join(names[1:]).split("#")
            if len(filenames) >= 2:
                filename = filenames[0]
                sheet_name = filenames[1]
            else:
                filename = names[1]
        return filename, sheet_name

    def ensure_open_file(self, name):
        if not name:
            raise ExeclFileNotFound()

        filename, sheet_name = self.parse_filename(name)
        if not filename:
            raise ExeclFileNotFound()

        if filename not in self.execls:
            if openpyxl is None:
                raise ImportError("openpyxl>=2.5.0 is required")

            filename = os.path.join(self.config["path"], filename)
            if filename in self.execl_fps:
                execl_fp = self.execl_fps[filename]
            elif os.path.exists(filename):
                self.execl_fps[filename] = execl_fp = openpyxl.load_workbook(filename)
            else:
                execl_fp = None

            if execl_fp:
                if sheet_name:
                    execl_sheet = None
                    for sheet in execl_fp.worksheets:
                        if sheet.title == sheet_name:
                            execl_sheet = sheet
                            break

                    if not execl_sheet:
                        execl_sheet = execl_fp.create_sheet(sheet_name)
                else:
                    worksheets = execl_fp.worksheets
                    if not worksheets:
                        execl_sheet = execl_fp.create_sheet()
                    else:
                        execl_sheet = worksheets[0]
            else:
                self.execl_fps[filename] = execl_fp = openpyxl.Workbook()
                execl_sheet = execl_fp.worksheets[0]
                if sheet_name:
                    execl_sheet.title = sheet_name

            self.execls[name] = ExeclSheet(name, filename, sheet_name, execl_fp, execl_sheet)
            self.execls[name].load()

        return self.execls[name]

    def query(self, name, *fields):
        return ExeclQueryBuilder(self, name, fields)

    def insert(self, name, datas):
        return ExeclInsertBuilder(self, name, datas)

    def update(self, name, **update):
        return ExeclUpdateBuilder(self, name, update)

    def delete(self, name):
        return ExeclDeleteBuilder(self, name)

    def close(self):
        if self.execls:
            for name, execl_sheet in self.execls.items():
                execl_sheet.close()

        if self.execl_fps:
            for filename, execl_fp in self.execl_fps.items():
                execl_fp.save(filename)

        self.execls = {}
        self.execl_fps = {}