# -*- coding: utf-8 -*-
# 2020/12/15
# create by: snower

from collections import OrderedDict
from .calculater import Calculater


class ConversionCalculater(Calculater):
    def get_xykey_value(self, xkeys, ykeyses, vkeys, data):
        xvalue, yvalue, vvalue = data, data, (data if vkeys else 1)
        for k in xkeys:
            if not isinstance(xvalue, dict) or k not in xvalue:
                xvalue = None
                break
            xvalue = xvalue[k]

        yvalues = []
        for ykeys in ykeyses:
            for k in ykeys:
                if not isinstance(yvalue, dict) or k not in yvalue:
                    yvalue = None
                    break
                yvalue = yvalue[k]
            yvalues.append(yvalue)
            yvalue = data

        for k in vkeys:
            if not isinstance(vvalue, dict) or k not in vvalue:
                vvalue = 0
                break
            vvalue = vvalue[k]
        return xvalue, tuple(yvalues) if [yvalue for yvalue in yvalues if yvalue is not None] else None, vvalue

    def update_outputer_schema(self, xtitles):
        from ..taskers.tasker import current_tasker
        tasker = current_tasker()
        tasker.outputer.schema = OrderedDict()
        for ykey in (self.args[2] if isinstance(self.args[2], list) else [self.args[2]]):
            valuer = tasker.create_valuer(tasker.compile_db_valuer(ykey, None))
            if valuer:
                tasker.outputer.add_valuer(ykey, valuer)
        for xvalue in xtitles:
            valuer = tasker.create_valuer(tasker.compile_db_valuer(xvalue, None))
            if valuer:
                tasker.outputer.add_valuer(xvalue, valuer)

    def calculate(self):
        if len(self.args) < 3:
            return None

        xkeys = str(self.args[1]).split(".")
        ykeyses = [str(key).split(".") for key in self.args[2]] if isinstance(self.args[2], list) \
            else [str(self.args[2]).split(".")]
        vkeys = str(self.args[3]).split(".") if len(self.args) >= 4 else []

        xtitles, ytitles, values = OrderedDict(), OrderedDict(), {}
        for data in self.args[0]:
            xvalue, yvalues, vvalue = self.get_xykey_value(xkeys, ykeyses, vkeys, data)
            if xvalue is None or not yvalues:
                continue

            xtitles[xvalue] = True
            ytitles[yvalues] = True
            if xvalue not in values:
                values[xvalue] = {}

            if yvalues not in values[xvalue]:
                values[xvalue][yvalues] = vvalue
            else:
                values[xvalue][yvalues] += vvalue

        datas = []
        for yvalues in ytitles:
            data = OrderedDict()
            for ykey, yvaue in zip(self.args[2] if isinstance(self.args[2], list) else [self.args[2]], yvalues):
                data[ykey] = yvaue
            for xvalue in xtitles:
                if xvalue in values and yvalues in values[xvalue]:
                    data[xvalue] = values[xvalue][yvalues]
                else:
                    data[xvalue] = 0
            datas.append(data)
        self.update_outputer_schema(xtitles)
        return datas