# -*- coding: utf-8 -*-
# 2020/12/15
# create by: snower

from .calculater import Calculater


class ConvV4HCalculater(Calculater):
    def calculate(self):
        if len(self.args) < 3:
            return None

        datas = self.args[0] if isinstance(self.args[0], list) else \
            ([self.args[0]] if isinstance(self.args[0], dict) else [])
        key, vkey = self.args[1], self.args[2]
        if len(self.args) >= 4:
            reserved_keys = set(self.args[3] if isinstance(self.args[3], list) else [self.args[3]])
        else:
            reserved_keys = set([])
        reserved_keys.add(key)
        reserved_keys.add(vkey)
        result, reserved_data = [], {}
        for data in datas:
            for k, v in data.items():
                if k in reserved_keys:
                    reserved_data[k] = v
            for k, v in data.items():
                if k not in reserved_keys:
                    reserved_data[key] = k
                    reserved_data[vkey] = v
                    result.append(dict(**reserved_data))
        return result


class ConvH4VCalculater(Calculater):
    def calculate(self):
        if len(self.args) < 3:
            return None

        vhkey, vvkey = self.args[1], self.args[2]
        vcount_key = self.args[3] if len(self.args) >= 4 else None
        vcount_callable = callable(vcount_key) if vcount_key else False
        datas = self.args[0] if isinstance(self.args[0], list) else \
            ([self.args[0]] if isinstance(self.args[0], dict) else [])

        hkeys, vkeys, mdata = {}, {}, {}
        for data in datas:
            if vhkey not in data:
                continue
            if vvkey not in data:
                continue
            hvalue, vvalue = data[vhkey], data[vvkey]
            if hvalue not in hkeys:
                hkeys[hvalue] = True
            if vvalue not in vkeys:
                vkeys[vvalue] = True
            if hvalue not in mdata:
                mdata[hvalue] = {}
            if not vcount_key:
                if vvalue not in mdata[hvalue]:
                    mdata[hvalue][vvalue] = 1
                else:
                    mdata[hvalue][vvalue] += 1
                continue

            if vcount_callable:
                if vvalue not in mdata[hvalue]:
                    mdata[hvalue][vvalue] = vcount_key(dict(value=0, data=data))
                else:
                    mdata[hvalue][vvalue] = vcount_key(dict(value=mdata[hvalue][vvalue], data=data))
            elif vcount_key in data and isinstance(data[vcount_key], (int, float)):
                if vvalue not in mdata[hvalue]:
                    mdata[hvalue][vvalue] = data[vcount_key]
                else:
                    mdata[hvalue][vvalue] += data[vcount_key]

        result = []
        for vkey in vkeys:
            data = {vvkey: vkey}
            for hkey in hkeys:
                data[hkey] = mdata[hkey][vkey] if hkey in mdata and vkey in mdata[hkey] else 0
            result.append(data)
        return result


class ConvV2HCalculater(Calculater):
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
        if len(self.args) < 3:
            return None

        vhkey, vvkey = self.args[1], self.args[2]
        vcount_key = self.args[3] if len(self.args) >= 4 else None
        vcount_callable = callable(vcount_key) if vcount_key else False
        datas = self.args[0] if isinstance(self.args[0], list) else \
            ([self.args[0]] if isinstance(self.args[0], dict) else [])

        hkeys, vkeys, mdata = {}, {}, {}
        for data in datas:
            if vhkey not in data:
                continue
            if vvkey not in data:
                continue
            hvalue, vvalue = data[vhkey], data[vvkey]
            if hvalue not in hkeys:
                hkeys[hvalue] = True
            if vvalue not in vkeys:
                vkeys[vvalue] = True
            if hvalue not in mdata:
                mdata[hvalue] = {}
            if not vcount_key:
                if vvalue not in mdata[hvalue]:
                    mdata[hvalue][vvalue] = 1
                else:
                    mdata[hvalue][vvalue] += 1
                continue

            if vcount_callable:
                if vvalue not in mdata[hvalue]:
                    mdata[hvalue][vvalue] = vcount_key(dict(value=0, data=data))
                else:
                    mdata[hvalue][vvalue] = vcount_key(dict(value=mdata[hvalue][vvalue], data=data))
            elif vcount_key in data and isinstance(data[vcount_key], (int, float)):
                if vvalue not in mdata[hvalue]:
                    mdata[hvalue][vvalue] = data[vcount_key]
                else:
                    mdata[hvalue][vvalue] += data[vcount_key]

        result = []
        for vkey in vkeys:
            data = {vvkey: vkey}
            for hkey in hkeys:
                data[hkey] = mdata[hkey][vkey] if hkey in mdata and vkey in mdata[hkey] else 0
            result.append(data)
        self.update_outputer_schema([vvkey] + list(hkeys.keys()))
        return result


class ConvH2VCalculater(Calculater):
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
        if len(self.args) < 3:
            return None

        datas = self.args[0] if isinstance(self.args[0], list) else \
            ([self.args[0]] if isinstance(self.args[0], dict) else [])
        key, vkey = self.args[1], self.args[2]
        if len(self.args) >= 4:
            reserved_keys = set(self.args[3] if isinstance(self.args[3], list) else [self.args[3]])
        else:
            reserved_keys = set([])
        reserved_keys.add(key)
        reserved_keys.add(vkey)
        result, reserved_data = [], {}
        for data in datas:
            for k, v in data.items():
                if k in reserved_keys:
                    reserved_data[k] = v
            for k, v in data.items():
                if k not in reserved_keys:
                    reserved_data[key] = k
                    reserved_data[vkey] = v
                    result.append(dict(**reserved_data))
        self.update_outputer_schema(list(reserved_data.keys()))
        return result


class ConvUniqKVCalculater(Calculater):
    def calculate(self):
        if len(self.args) < 4:
            return None

        datas = self.args[0] if isinstance(self.args[0], list) else \
            ([self.args[0]] if isinstance(self.args[0], dict) else [])
        ukey, kkey, vkey = self.args[1], self.args[2], self.args[3]
        keys = set([])
        result, uniq_datas = [], {}
        for data in datas:
            if ukey not in data:
                continue

            uvalue = data[ukey]
            if uvalue not in uniq_datas:
                for k in data:
                    keys.add(k)
                uniq_datas[uvalue] = data
                result.append(data)

            uniq_data = uniq_datas[uvalue]
            if kkey in data and vkey in data:
                uniq_data[data[kkey]] = data[vkey]
                keys.add(data[kkey])

        from ..taskers.tasker import current_tasker
        tasker = current_tasker()
        valuer = tasker.outputer.schema[vkey]
        for data in result:
            for key in keys:
                if key in data:
                    continue
                data[key] = valuer.clone().fill(None).get()

        for key in keys:
            if key in tasker.outputer.schema:
                continue
            valuer = tasker.create_valuer(tasker.valuer_compiler.compile_data_valuer(key, None))
            if not valuer:
                continue
            tasker.outputer.add_valuer(key, valuer)
        return result


class ConvCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvCalculater, self).__init__(*args, **kwargs)

        if self.name == "conv::v4h":
            self.conv = ConvV4HCalculater(*args, **kwargs)
        elif self.name == "conv::h4v":
            self.conv = ConvH4VCalculater(*args, **kwargs)
        elif self.name == "conv::v2h":
            self.conv = ConvV2HCalculater(*args, **kwargs)
        elif self.name == "conv::h2v":
            self.conv = ConvH2VCalculater(*args, **kwargs)
        elif self.name == "conv::uniqkv":
            self.conv = ConvUniqKVCalculater(*args, **kwargs)
        else:
            self.conv = None

    def calculate(self):
        if self.conv:
            return self.conv.calculate()
        return None
