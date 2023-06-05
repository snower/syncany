# -*- coding: utf-8 -*-
# 2020/7/2
# create by: snower

from collections import defaultdict
from ..utils import CmpValue
from .valuer import Valuer


class PartitionCalculaterContext(object):
    def __init__(self):
        self.datas = None
        self.current_index = 0
        self.start_index = 0
        self.end_index = -1

    @property
    def current_data(self):
        return self.datas[self.current_index]


class OrderPartitionCalculaterContext(PartitionCalculaterContext):
    @property
    def current_data(self):
        return self.datas[self.current_index][1]

    @property
    def order_value(self):
        return self.datas[self.current_index][0]

    def update_index(self, current_index, start_index, end_index):
        self.current_index, self.start_index, self.end_index = current_index, start_index, end_index


class PartitionCalculater(object):
    calculate_valuer = None
    return_valuer = None
    value = None

    def __init__(self, value, calculate_valuer, return_valuer):
        self.value = value
        if calculate_valuer is not None:
            self.calculate_valuer = calculate_valuer
        if return_valuer is not None:
            self.return_valuer = return_valuer

    def calculate(self, data):
        self.value = data
        return self.calculate_valuer.fill_get(data)

    def get(self):
        if self.return_valuer:
            return self.return_valuer.fill_get(self.value["state"])
        return self.value["state"]


class OrderPartitionCalculater(object):
    calculate_valuer = None
    return_valuer = None
    value = None

    def __init__(self, value, calculate_valuer, return_valuer):
        self.value = value
        if calculate_valuer is not None:
            self.calculate_valuer = calculate_valuer
        if return_valuer is not None:
            self.return_valuer = return_valuer

    def calculate(self, data):
        value = self.calculate_valuer.fill_get(data)
        if self.return_valuer:
            self.value = self.return_valuer.fill_get(value)
        else:
            self.value = value
        return value

    def get(self):
        return self.value


class Partition(object):
    def __init__(self, key, options):
        self.key = key
        self.options = options
        self.calculates = defaultdict(list)
        self.datas = []
        self.states = {}

    def add_data(self, data, partition_calculater):
        data_id = id(data)
        if data_id not in self.calculates:
            self.datas.append(data)
        self.calculates[data_id].append(partition_calculater)
        if partition_calculater.calculate_valuer.valuer_id not in self.states:
            self.states[partition_calculater.calculate_valuer.valuer_id] = {
                "value": None,
                "state": None,
                "context": PartitionCalculaterContext()
            }

    def calculate(self):
        if not self.datas:
            return
        for state_data in self.states.values():
            state_data["context"].datas = self.datas
        for partition_calculaters in self.calculates.values():
            for partition_calculater in partition_calculaters:
                state_data = self.states[partition_calculater.calculate_valuer.valuer_id]
                state_data["value"] = partition_calculater.value
                state_data["state"] = partition_calculater.calculate(state_data)
        self.datas, self.calculates, self.states = [], defaultdict(list), {}


class OrderPartition(object):
    def __init__(self, key, options):
        self.key = key
        self.options = options
        self.calculates = defaultdict(list)
        self.datas = []
        self.states = {}

    def add_data(self, order_value, data, partition_calculater):
        data_id = id(data)
        if data_id not in self.calculates:
            self.datas.append((order_value, data))
        self.calculates[data_id].append(partition_calculater)
        if partition_calculater.calculate_valuer.valuer_id not in self.states:
            self.states[partition_calculater.calculate_valuer.valuer_id] = {
                "value": None,
                "state": None,
                "context": OrderPartitionCalculaterContext()
            }


    def calculate(self):
        if not self.datas:
            return
        if self.options.get("orders"):
            datas, self.datas = self.sort_datas(self.datas, self.options["orders"]), []
        else:
            datas, self.datas = sorted(self.datas, key=lambda x: x[0]), []
        for state_data in self.states.values():
            state_data["context"].datas = datas
        for i in range(len(datas)):
            for partition_calculater in self.calculates[id(datas[i][1])]:
                state_data = self.states[partition_calculater.calculate_valuer.valuer_id]
                state_data["context"].update_index(i, i, i + 1)
                state_data["value"] = partition_calculater.value
                state_data["state"] = partition_calculater.calculate(state_data)
        self.datas, self.calculates, self.states = [], defaultdict(list), {}

    def sort_datas(self, iterable, keys=None, reverse=None):
        if not keys:
            return sorted(iterable, reverse=True if reverse else False)
        if not isinstance(keys, (list, tuple, set)):
            keys = [keys]
        reverse_keys = [tuple(key) for key in keys if isinstance(key, (tuple, list, set)) and len(key) == 2
                        and isinstance(key[0], str) and key[1]]
        if reverse is None:
            reverse = True if len(reverse_keys) > len(keys) / 2 else False
        else:
            reverse = True if reverse else False
        sort_keys = []
        for key in keys:
            if isinstance(key, str):
                sort_key = (key.split("."), True if reverse else False)
            elif isinstance(key, (tuple, list, set)) and len(key) == 2 and isinstance(key[0], str):
                sort_key = (key[0].split("."), (False if key[1] else True) if reverse else (True if key[1] else False))
            else:
                raise TypeError("unknown keys type: " + str(keys))
            sort_keys.append(sort_key)
        if len(sort_keys) == 1 and len(sort_keys[0][0]) == 1:
            reverse = (not sort_keys[0][1]) if reverse else sort_keys[0][1]
            try:
                return sorted(iterable, key=lambda x: x[0], reverse=reverse)
            except:
                return sorted(iterable, key=lambda x: CmpValue(x[0]), reverse=reverse)

        def get_cmp_key(x):
            key_values = []
            for i in range(len(sort_keys)):
                key_value = x[i]
                if not sort_keys[i][1]:
                    key_values.append(key_value)
                else:
                    key_values.append(-key_value if isinstance(key_value, (int, float)) else CmpValue(key_value, True))
            return tuple(key_values)

        try:
            return sorted(iterable, key=get_cmp_key, reverse=reverse)
        except:
            def get_format_cmp_key(x):
                key_values = []
                for i in range(len(sort_keys)):
                    key_value = x[i]
                    key_values.append(CmpValue(key_value, False if not sort_keys[i][1] else True))
                return tuple(key_values)
            return sorted(iterable, key=get_format_cmp_key, reverse=reverse)


class PartitionManager(object):
    def __init__(self, options):
        self.options = options
        self.partitions = {}

    def get_partition(self, key):
        return self.partitions.get(key)

    def add_partition(self, key):
        partition = Partition(key, self.options)
        self.partitions[key] = partition
        return partition

    def add_order_partition(self, key):
        partition = OrderPartition(key, self.options)
        self.partitions[key] = partition
        return partition

    def reset(self):
        self.partitions.clear()


class PartitionValuer(Valuer):
    def __init__(self, key_valuer, order_valuer, value_valuer, calculate_valuer, return_valuer, inherit_valuers, partition_manager, *args, **kwargs):
        self.key_valuer = key_valuer
        self.order_valuer = order_valuer
        self.value_valuer = value_valuer
        self.calculate_valuer = calculate_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.partition_manager = partition_manager
        super(PartitionValuer, self).__init__(*args, **kwargs)

    @classmethod
    def create_manager(cls, options):
        return PartitionManager(options)

    def new_init(self):
        super(PartitionValuer, self).new_init()
        self.value_wait_loaded = True if (self.key_valuer and self.key_valuer.require_loaded()) \
                                             or (self.order_valuer and self.order_valuer.require_loaded()) \
                                                 or (self.value_valuer and self.value_valuer.require_loaded()) else False

    def clone_init(self, from_valuer):
        super(PartitionValuer, self).clone_init(from_valuer)
        self.value_wait_loaded = from_valuer.value_wait_loaded

    def get_manager(self):
        return self.partition_manager

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_loader(self, is_return_getter=False, partition_valuers=None, **kwargs):
        if partition_valuers is None:
            partition_valuers = []
        partition_valuers.append(self)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_loader(is_return_getter=False, partition_valuers=partition_valuers, **kwargs)
        if self.key_valuer:
            self.key_valuer.mount_loader(is_return_getter=False, partition_valuers=partition_valuers, **kwargs)
        if self.order_valuer:
            self.order_valuer.mount_loader(is_return_getter=False, partition_valuers=partition_valuers, **kwargs)
        if self.value_valuer:
            self.value_valuer.mount_loader(is_return_getter=False, partition_valuers=partition_valuers, **kwargs)
        if self.calculate_valuer:
            self.calculate_valuer.mount_loader(is_return_getter=False, partition_valuers=partition_valuers, **kwargs)
        if self.return_valuer:
            self.return_valuer.mount_loader(is_return_getter=is_return_getter and True,
                                            partition_valuers=partition_valuers, **kwargs)

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        key_valuer = self.key_valuer.clone(contexter, **kwargs) if self.key_valuer else None
        order_valuer = self.order_valuer.clone(contexter, **kwargs) if self.order_valuer else None
        value_valuer = self.value_valuer.clone(contexter, **kwargs) if self.value_valuer else None
        calculate_valuer = self.calculate_valuer.clone(contexter, **kwargs) if self.calculate_valuer else None
        return_valuer = self.return_valuer.clone(contexter, **kwargs) if self.return_valuer else None
        if contexter is not None:
            return ContextPartitionValuer(key_valuer, order_valuer, value_valuer, calculate_valuer, return_valuer, inherit_valuers,
                                         self.partition_manager, self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextPartitionValuer):
            return ContextPartitionValuer(key_valuer, order_valuer, value_valuer, calculate_valuer, return_valuer, inherit_valuers,
                                         self.partition_manager, self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(key_valuer, order_valuer, value_valuer, calculate_valuer, return_valuer, inherit_valuers,
                              self.partition_manager, self.key, self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if not self.value_wait_loaded:
            self.value = (self.key_valuer.fill_get(data) if self.key_valuer else None,
                          self.order_valuer.fill_get(data) if self.order_valuer else None,
                          self.value_valuer.fill_get(data) if self.value_valuer else None)
            return self
        if self.key_valuer:
            self.key_valuer.fill(data)
        if self.order_valuer:
            self.order_valuer.fill(data)
        if self.value_valuer:
            self.value_valuer.fill(data)
        return self

    def get(self):
        if not self.value_wait_loaded:
            key_value, order_value, value = self.value
        else:
            key_value = self.key_valuer.get() if self.key_valuer else ""
            order_value = self.order_valuer.get() if self.order_valuer else None
            value = self.value_valuer.get() if self.value_valuer else None

        def calculate_value(cdata):
            if self.order_valuer:
                partition = self.partition_manager.get_partition(key_value)
                if partition is None:
                    partition = self.partition_manager.add_order_partition(key_value)
                partition_calculater = OrderPartitionCalculater(value, self.calculate_valuer, self.return_valuer)
                partition.add_data(order_value, cdata, partition_calculater)
            else:
                partition = self.partition_manager.get_partition(key_value)
                if partition is None:
                    partition = self.partition_manager.add_partition(key_value)
                partition_calculater = PartitionCalculater(value, self.calculate_valuer, self.return_valuer)
                partition.add_data(cdata, partition_calculater)

            def partition_value():
                partition.calculate()
                return partition_calculater.get()
            return partition_value
        return calculate_value

    def fill_get(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        key_value = self.key_valuer.fill_get(data) if self.key_valuer else ""
        order_value = self.order_valuer.fill_get(data) if self.order_valuer else None
        value = self.value_valuer.fill_get(data) if self.value_valuer else None

        def calculate_value(cdata):
            if self.order_valuer:
                partition = self.partition_manager.get_partition(key_value)
                if partition is None:
                    partition = self.partition_manager.add_order_partition(key_value)
                partition_calculater = OrderPartitionCalculater(value, self.calculate_valuer, self.return_valuer)
                partition.add_data(order_value, cdata, partition_calculater)
            else:
                partition = self.partition_manager.get_partition(key_value)
                if partition is None:
                    partition = self.partition_manager.add_partition(key_value)
                partition_calculater = PartitionCalculater(value, self.calculate_valuer, self.return_valuer)
                partition.add_data(cdata, partition_calculater)

            def partition_value():
                partition.calculate()
                return partition_calculater.get()
            return partition_value
        return calculate_value

    def reset(self):
        self.partition_manager.reset()
        super(PartitionValuer, self).reset()

    def childs(self):
        childs = []
        if self.key_valuer:
            childs.append(self.key_valuer)
        if self.order_valuer:
            childs.append(self.key_valuer)
        if self.value_valuer:
            childs.append(self.value_valuer)
        if self.calculate_valuer:
            childs.append(self.calculate_valuer)
        if self.return_valuer:
            childs.append(self.key_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        fields = []
        if self.key_valuer:
            for field in self.key_valuer.get_fields():
                fields.append(field)

        if self.order_valuer:
            for field in self.order_valuer.get_fields():
                fields.append(field)

        if self.value_valuer:
            for field in self.value_valuer.get_fields():
                fields.append(field)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        if self.return_valuer:
            return self.return_valuer.get_final_filter()

        if self.filter:
            return self.filter

        if self.calculate_valuer:
            return self.calculate_valuer.get_final_filter()
        return None

    def is_aggregate(self):
        return True

    def is_yield(self):
        return False


class ContextPartitionValuer(PartitionValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextPartitionValuer, self).__init__(*args, **kwargs)

    @property
    def value(self):
        try:
            return self.contexter.values[self.value_context_id]
        except KeyError:
            return None

    @value.setter
    def value(self, v):
        if v is None:
            if self.value_context_id in self.contexter.values:
                self.contexter.values.pop(self.value_context_id)
            return
        self.contexter.values[self.value_context_id] = v

