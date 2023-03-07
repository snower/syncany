# -*- coding: utf-8 -*-
# 2023/2/19
# create by: snower

from .calculater import Calculater
from ..filters.builtin import *

class ConvertIntCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

        filter = IntFilter(*tuple(self.args[1:]))
        if isinstance(self.args[0], list):
            result = []
            for data in self.args[0]:
                try:
                    value = int(data)
                except:
                    value = filter.filter(data)
                    if value == 0:
                        continue
                result.append(value)
            return result if result else [0]
        return filter.filter(self.args[0])


class ConvertFloatCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0.0

        filter = FloatFilter(*tuple(self.args[1:]))
        if isinstance(self.args[0], list):
            result = []
            for data in self.args[0]:
                try:
                    value = float(data)
                except:
                    value = filter.filter(data)
                    if value == 0:
                        continue
                result.append(value)
            return result if result else [0.0]
        return filter.filter(self.args[0])


class ConvertStringCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return ''

        filter = StringFilter(*tuple(self.args[1:]))
        if isinstance(self.args[0], list):
            return [filter.filter(data) for data in self.args[0]]
        return filter.filter(self.args[0])


class ConvertBytesCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return b''

        filter = BytesFilter(*tuple(self.args[1:]))
        if isinstance(self.args[0], list):
            return [filter.filter(data) for data in self.args[0]]
        return filter.filter(self.args[0])


class ConvertBooleanCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return False

        filter = BooleanFilter(*tuple(self.args[1:]))
        if isinstance(self.args[0], list):
            return [filter.filter(data) for data in self.args[0]]
        return filter.filter(self.args[0])


class ConvertArrayCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return []
        return ArrayFilter(*tuple(self.args[1:])).filter(self.args[0])


class ConvertMapCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return {}
        return MapFilter(*tuple(self.args[1:])).filter(self.args[0])


class ConvertObjectIdCalculater(Calculater):
    def calculate(self):
        if ObjectId is None:
            raise ImportError(u"bson required")

        default_value = ObjectId("000000000000000000000000")
        if not self.args:
            return default_value

        filter = ObjectIdFilter(*tuple(self.args[1:]))
        if isinstance(self.args[0], list):
            result = []
            for data in self.args[0]:
                try:
                    value = ObjectId(data)
                except:
                    value = filter.filter(data)
                    if value == default_value:
                        continue
                result.append(value)
            return result if result else [default_value]
        return filter.filter(self.args[0])


class ConvertUUIDCalculater(Calculater):
    def calculate(self):
        default_value = uuid.UUID("00000000-0000-0000-0000-000000000000")
        if not self.args:
            return default_value

        filter = UUIDFilter(*tuple(self.args[1:]))
        if isinstance(self.args[0], list):
            result = []
            for data in self.args[0]:
                try:
                    value = uuid.UUID(data)
                except:
                    value = filter.filter(data)
                    if value == default_value:
                        continue
                result.append(value)
            return result if result else [default_value]
        return filter.filter(self.args[0])


class ConvertDateTimeCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        filter = DateTimeFilter(*tuple(self.args[1:]))
        if isinstance(self.args[0], list):
            result = []
            for data in self.args[0]:
                value = filter.filter(data)
                if value is None:
                    continue
                result.append(value)
            return result if result else [None]
        return filter.filter(self.args[0])


class ConvertDateCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        filter = DateFilter(*tuple(self.args[1:]))
        if isinstance(self.args[0], list):
            result = []
            for data in self.args[0]:
                value = filter.filter(data)
                if value is None:
                    continue
                result.append(value)
            return result if result else [None]
        return filter.filter(self.args[0])


class ConvertTimeCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        filter = TimeFilter(*tuple(self.args[1:]))
        if isinstance(self.args[0], list):
            result = []
            for data in self.args[0]:
                value = filter.filter(data)
                if value is None:
                    continue
                result.append(value)
            return result if result else [None]
        return filter.filter(self.args[0])
