# -*- coding: utf-8 -*-
# 2020/12/15
# create by: snower

from .calculater import Calculater


class TransformCalculater(Calculater):
    def update_outputer_schema(self, keys):
        from ..taskers.tasker import current_tasker
        tasker = current_tasker()
        tasker.outputer.schema = {}
        for key in keys:
            if not isinstance(key, str):
                continue
            valuer = tasker.create_valuer(tasker.valuer_compiler.compile_data_valuer(key, None))
            if not valuer:
                continue
            tasker.outputer.add_valuer(key, valuer)


class TransformV4HCalculater(TransformCalculater):
    """
    转为kEY-VALUE纵向表
        参数1：key
        参数2：value

    如以下表格：
     --------------------
    | id | name   |  age |
     --------------------
    | 1  | limei  |  18  |
    | 2  | wanzhi |  22  |
     --------------------

    经过transform::v4h('key', 'value', 'name')后变为：

     -------------------------
    | key   | value  | name   |
     -------------------------
    | id    | 1      | limei  |
    | age   | 18     | limei  |
    | id    | 2      | wanzhi |
    | age   | 22     | wanzhi |
     -------------------------
    """

    def calculate(self, *args):
        if len(args) < 3:
            return []

        datas = args[0] if isinstance(args[0], list) else \
            ([args[0]] if isinstance(args[0], dict) else [])
        key, vkey = args[1], args[2]
        reserved_keys = [key, vkey]
        if len(args) >= 4:
            reserved_keys.extend(list({str(k) for k in args[3]}) if isinstance(args[3], list) else [str(args[3])])
        result, reserved_data = [], {key: None, vkey: None}
        for data in datas:
            for k, v in data.items():
                if k not in reserved_keys:
                    continue
                reserved_data[k] = v
            for k, v in data.items():
                if k in reserved_keys:
                    continue
                reserved_data[key] = k
                reserved_data[vkey] = v
                result.append(dict(**reserved_data))

        self.update_outputer_schema(reserved_keys)
        return result


class TransformH4VCalculater(TransformCalculater):
    """
    把kEY-VALUE转为横向表

    如以下表格：
     ----------------------
    | key      | name   | value |
     ----------------------
    | order_id | limei  | 1     |
    | goods    | limei  | 青菜   |
    | age      | limei  | 18    |
    | order_id | wanzhi | 2     |
    | goods    | wanzhi | 白菜   |
    | age      | wanzhi | 22    |
    | order_id | wanzhi | 3     |
    | goods    | wanzhi | 青菜   |
    | age      | wanzhi | 22    |
     ----------------------

    经过transform::h4v('name', 'key', 'value')后变为：

     ------------------------
    | name | id | goods | age |
     -------------------------
    | limei  | 1  | 青菜 | 18  |
    | wanzhi | 2  | 白菜 | 22  |
    | wanzhi | 2  | 青菜 | 22  |
     ------------------------
    """

    def calculate(self, *args):
        if len(args) < 3:
            return []

        vhkey, vvkey = args[1], args[2]
        vikey = args[3] if len(args) >= 4 else None
        vvkey_callable = callable(vvkey) if vvkey else False
        datas = args[0] if isinstance(args[0], list) else \
            ([args[0]] if isinstance(args[0], dict) else [])

        vkeys, ivalue, rdata, result = ([] if vikey is None else [vikey]), None, None, []
        for data in datas:
            if vhkey not in data:
                continue
            if vvkey not in data:
                continue
            hvalue, vvalue, vivalue = data[vhkey], (vvkey(data) if vvkey_callable else data[vvkey]), (data[vikey] if vikey in data else None)
            if (ivalue is None and rdata is None) or hvalue in rdata or (vikey is not None and vivalue != ivalue):
                if rdata is not None:
                    result.append(rdata)
                ivalue, rdata = vivalue, ({} if vikey is None else {vikey: vivalue})
            rdata[hvalue] = vvalue
            if hvalue not in vkeys:
                vkeys.append(str(hvalue))
        if rdata:
            result.append(rdata)

        self.update_outputer_schema(vkeys)
        return result


class TransformV2HCalculater(TransformCalculater):
    """
    纵向表转为横向表
        参数1：横向表头
        参数2：纵向统计值
        参数3：值，相同位置如有多个值数字则加和，否则最后一个值有效，无第三个参数时统计数量

    如以下表格：
     -------------------------------------
    | id | name   |  order_date  | amount |
     -------------------------------------
    | 1  | limei  |  2022-01-01  | 5.5    |
    | 2  | wanzhi |  2022-01-01  | 8.2    |
     -------------------------------------

    经过transform::v2h('name', 'order_date', 'amount')后变为：

     --------------------------------
    | order_date   | limei  | wanzhi |
     --------------------------------
    | 2022-01-01   | 5.5    | 8.2    |
     --------------------------------
    """

    def calculate(self, *args):
        if len(args) < 3:
            return []

        vhkey, vvkey = args[1], args[2]
        vvalue_key = args[3] if len(args) >= 4 else None
        vvalue_callable = callable(vvalue_key) if vvalue_key else False
        datas = args[0] if isinstance(args[0], list) else \
            ([args[0]] if isinstance(args[0], dict) else [])

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
            if not vvalue_key:
                if vvalue not in mdata[hvalue]:
                    mdata[hvalue][vvalue] = 1
                else:
                    mdata[hvalue][vvalue] += 1
                continue

            if vvalue_callable:
                if vvalue not in mdata[hvalue]:
                    mdata[hvalue][vvalue] = vvalue_key(dict(value=0, data=data))
                else:
                    mdata[hvalue][vvalue] = vvalue_key(dict(value=mdata[hvalue][vvalue], data=data))
            elif vvalue_key in data:
                if data[vvalue_key] is None and vvalue in mdata[hvalue]:
                    continue
                if isinstance(data[vvalue_key], (int, float)):
                    if vvalue not in mdata[hvalue]:
                        mdata[hvalue][vvalue] = data[vvalue_key]
                    else:
                        mdata[hvalue][vvalue] += data[vvalue_key]
                else:
                    mdata[hvalue][vvalue] = data[vvalue_key]
            elif vvalue not in mdata[hvalue]:
                mdata[hvalue][vvalue] = 0

        result = []
        for vkey in vkeys:
            data = {vvkey: vkey}
            for hkey in hkeys:
                data[hkey] = mdata[hkey][vkey] if hkey in mdata and vkey in mdata[hkey] else 0
            result.append(data)
        self.update_outputer_schema([str(vvkey)] + [str(hkey) for hkey in hkeys.keys()])
        return result


class TransformH2VCalculater(TransformCalculater):
    """
    横向表转为纵向表
        参数1：横向表头
        参数2：纵向统计值
        参数3：值，不传递不保留值

    如以下表格：
     --------------------------------
    | order_date   | limei  | wanzhi |
     --------------------------------
    | 2022-01-01   | 5.5    | 8.2    |
    | 2022-01-02   | 4.3    | 1.8    |
     --------------------------------

    经过transform::h2v('name', 'order_date', 'amount')后变为：

    --------------------------------
    | name   |  order_date  | amount |
    --------------------------------
    | limei  |  2022-01-01  | 5.5    |
    | wanzhi |  2022-01-01  | 8.2    |
    | limei  |  2022-01-02  | 4.3    |
    | wanzhi |  2022-01-02  | 1.8    |
     --------------------------------
    """

    def calculate(self, *args):
        if len(args) < 3:
            return []

        datas = args[0] if isinstance(args[0], list) else \
            ([args[0]] if isinstance(args[0], dict) else [])
        hkey, vkey, vvkey = args[1], args[2], (args[3] if len(args) >= 4 else None)

        result, reserved_data = [], {}
        for data in datas:
            reserved_data[vkey] = data.get(vkey)
            for k, v in data.items():
                if k == vkey:
                    continue
                reserved_data[hkey] = k
                if vvkey:
                    reserved_data[vvkey] = v
                result.append(dict(**reserved_data))
        self.update_outputer_schema([str(key) for key in reserved_data.keys()])
        return result


class TransformUniqKVCalculater(TransformCalculater):
    """
    去重，重复行横向扩展
        参数1：重复字段key
        参数2：横向表头key
        参数3：值key, 相同位置如有多个值数字则加和，否则最后一个值有效，无第三个参数时统计数量

    如以下表格：
     ---------------------------------------------
    | id | name   |  order_date  | amount | goods |
     ---------------------------------------------
    | 1  | limei  |  2022-01-01  | 5.5    | 青菜   |
    | 2  | wanzhi |  2022-01-01  | 8.2    | 白菜   |
    | 3  | wanzhi |  2022-01-01  | 2.2    | 青菜   |
     ---------------------------------------------

    经过transform::v2h('order_date', 'name', 'amount')后变为：

     -------------------------------------------------------------
    | id | name   |  order_date  | amount | goods | limei | wanzhi |
     ---------------------------------------------
    | 1  | limei  |  2022-01-01  | 5.5    | 青菜   | 5.5   | 10.4    |
     --------------------------------------------------------------
    """

    def calculate(self, *args):
        if len(args) < 3:
            return []

        datas = args[0] if isinstance(args[0], list) else \
            ([args[0]] if isinstance(args[0], dict) else [])
        ukey, kkey, vkey = args[1], args[2], (args[3] if len(args) >= 4 else None)
        keys = []
        result, uniq_datas = [], {}
        for data in datas:
            if ukey not in data:
                continue

            uvalue = data[ukey]
            if uvalue not in uniq_datas:
                for k in data:
                    if k in keys:
                        continue
                    keys.append(k)
                uniq_datas[uvalue] = data
                result.append(data)

            uniq_data, data_key = uniq_datas[uvalue], None
            if not vkey:
                if kkey not in data:
                    continue
                data_key = str(data[kkey])
                if data_key not in uniq_data:
                    uniq_data[data_key] = 1
                else:
                    uniq_data[data_key] += 1
                if data_key not in keys:
                    keys.append(data_key)
                continue

            if kkey in data and vkey in data:
                data_key = str(data[kkey])
                if data[vkey] is None and data_key in uniq_data:
                    continue
                if isinstance(data[vkey], (int, float)):
                    if data_key in uniq_data:
                        uniq_data[data_key] += data[vkey]
                    else:
                        uniq_data[data_key] = data[vkey]
                else:
                    uniq_data[data_key] = data[vkey]
                if data_key not in keys:
                    keys.append(data_key)

        from ..taskers.tasker import current_tasker
        tasker = current_tasker()
        valuer = tasker.outputer.schema[vkey] if vkey else None
        for data in result:
            for key in keys:
                if key in data:
                    continue
                if not valuer:
                    data[key] = 0
                    continue
                data[key] = valuer.reinit().fill(None).get()

        for key in keys:
            if key in tasker.outputer.schema:
                continue
            valuer = tasker.create_valuer(tasker.valuer_compiler.compile_data_valuer(key, None))
            if not valuer:
                continue
            tasker.outputer.add_valuer(key, valuer)
        return result


class TransformVHKCalculater(TransformCalculater):
    def __init__(self, *args, **kwargs):
        super(TransformVHKCalculater, self).__init__(*args, **kwargs)

        if self.name == "transform::v4h":
            self.transform = TransformV4HCalculater(*args, **kwargs)
        elif self.name == "transform::h4v":
            self.transform = TransformH4VCalculater(*args, **kwargs)
        elif self.name == "transform::v2h":
            self.transform = TransformV2HCalculater(*args, **kwargs)
        elif self.name == "transform::h2v":
            self.transform = TransformH2VCalculater(*args, **kwargs)
        elif self.name == "transform::uniqkv":
            self.transform = TransformUniqKVCalculater(*args, **kwargs)
        else:
            self.transform = None

    def calculate(self, *args):
        if self.transform:
            return self.transform.calculate(*args)
        return None
