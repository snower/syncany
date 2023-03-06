# -*- coding: utf-8 -*-
# 2023/2/19
# create by: snower

from .calculater import Calculater
from ..filters.builtin import *

class ConvertIntCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0

        if len(self.args) >= 1 and isinstance(self.args[0], list):
            datas = []
            for data in self.args[0]:
                if isinstance(data, dict):
                    if isinstance(self.args[1], str) and self.args[1] in data:
                        datas.append(IntFilter(*tuple(self.args[2:])).filter(data[self.args[1]]))
                else:
                    datas.append(IntFilter(*tuple(self.args[1:])).filter(self.args[0]))
            return datas
        return IntFilter(*tuple(self.args[1:])).filter(self.args[0])


class ConvertFloatCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return 0.0

        if len(self.args) >= 1 and isinstance(self.args[0], list):
            datas = []
            for data in self.args[0]:
                if isinstance(data, dict):
                    if isinstance(self.args[1], str) and self.args[1] in data:
                        datas.append(FloatFilter(*tuple(self.args[2:])).filter(data[self.args[1]]))
                else:
                    datas.append(FloatFilter(*tuple(self.args[1:])).filter(self.args[0]))
            return datas
        return FloatFilter(*tuple(self.args[1:])).filter(self.args[0])


class ConvertStringCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return ''

        if len(self.args) >= 1 and isinstance(self.args[0], list):
            datas = []
            for data in self.args[0]:
                if isinstance(data, dict):
                    if isinstance(self.args[1], str) and self.args[1] in data:
                        datas.append(StringFilter(*tuple(self.args[2:])).filter(data[self.args[1]]))
                else:
                    datas.append(StringFilter(*tuple(self.args[1:])).filter(self.args[0]))
            return datas
        return StringFilter(*tuple(self.args[1:])).filter(self.args[0])


class ConvertBytesCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return b''

        if len(self.args) >= 1 and isinstance(self.args[0], list):
            datas = []
            for data in self.args[0]:
                if isinstance(data, dict):
                    if isinstance(self.args[1], str) and self.args[1] in data:
                        datas.append(BytesFilter(*tuple(self.args[2:])).filter(data[self.args[1]]))
                else:
                    datas.append(BytesFilter(*tuple(self.args[1:])).filter(self.args[0]))
            return datas
        return BytesFilter(*tuple(self.args[1:])).filter(self.args[0])


class ConvertBooleanCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return False

        if len(self.args) >= 1 and isinstance(self.args[0], list):
            datas = []
            for data in self.args[0]:
                if isinstance(data, dict):
                    if isinstance(self.args[1], str) and self.args[1] in data:
                        datas.append(BooleanFilter(*tuple(self.args[2:])).filter(data[self.args[1]]))
                else:
                    datas.append(BooleanFilter(*tuple(self.args[1:])).filter(self.args[0]))
            return datas
        return BooleanFilter(*tuple(self.args[1:])).filter(self.args[0])


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

        if not self.args:
            return ObjectId("000000000000000000000000")

        if len(self.args) >= 1 and isinstance(self.args[0], list):
            datas = []
            for data in self.args[0]:
                if isinstance(data, dict):
                    if isinstance(self.args[1], str) and self.args[1] in data:
                        datas.append(ObjectIdFilter(*tuple(self.args[2:])).filter(data[self.args[1]]))
                else:
                    datas.append(ObjectIdFilter(*tuple(self.args[1:])).filter(self.args[0]))
            return datas
        return ObjectIdFilter(*tuple(self.args[1:])).filter(self.args[0])


class ConvertUUIDCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return uuid.UUID("00000000-0000-0000-0000-000000000000")

        if len(self.args) >= 1 and isinstance(self.args[0], list):
            datas = []
            for data in self.args[0]:
                if isinstance(data, dict):
                    if isinstance(self.args[1], str) and self.args[1] in data:
                        datas.append(UUIDFilter(*tuple(self.args[2:])).filter(data[self.args[1]]))
                else:
                    datas.append(UUIDFilter(*tuple(self.args[1:])).filter(self.args[0]))
            return datas
        return UUIDFilter(*tuple(self.args[1:])).filter(self.args[0])


class ConvertDateTimeCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        if len(self.args) >= 1 and isinstance(self.args[0], list):
            datas = []
            for data in self.args[0]:
                if isinstance(data, dict):
                    if isinstance(self.args[1], str) and self.args[1] in data:
                        datas.append(DateTimeFilter(*tuple(self.args[2:])).filter(data[self.args[1]]))
                else:
                    datas.append(DateTimeFilter(*tuple(self.args[1:])).filter(self.args[0]))
            return datas
        return DateTimeFilter(*tuple(self.args[1:])).filter(self.args[0])


class ConvertDateCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        if len(self.args) >= 1 and isinstance(self.args[0], list):
            datas = []
            for data in self.args[0]:
                if isinstance(data, dict):
                    if isinstance(self.args[1], str) and self.args[1] in data:
                        datas.append(DateFilter(*tuple(self.args[2:])).filter(data[self.args[1]]))
                else:
                    datas.append(DateFilter(*tuple(self.args[1:])).filter(self.args[0]))
            return datas
        return DateFilter(*tuple(self.args[1:])).filter(self.args[0])


class ConvertTimeCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        if len(self.args) >= 1 and isinstance(self.args[0], list):
            datas = []
            for data in self.args[0]:
                if isinstance(data, dict):
                    if isinstance(self.args[1], str) and self.args[1] in data:
                        datas.append(TimeFilter(*tuple(self.args[2:])).filter(data[self.args[1]]))
                else:
                    datas.append(TimeFilter(*tuple(self.args[1:])).filter(self.args[0]))
            return datas
        return TimeFilter(*tuple(self.args[1:])).filter(self.args[0])
