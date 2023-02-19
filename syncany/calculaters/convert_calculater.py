# -*- coding: utf-8 -*-
# 2023/2/19
# create by: snower

from .calculater import Calculater
from ..filters.builtin import *

class ConvertIntCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None
        return IntFilter(*tuple(self.args[1:])).filter(self.args[0])

class ConvertFloatCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None
        return FloatFilter(*tuple(self.args[1:])).filter(self.args[0])

class ConvertStringCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None
        return StringFilter(*tuple(self.args[1:])).filter(self.args[0])

class ConvertBytesCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None
        return BytesFilter(*tuple(self.args[1:])).filter(self.args[0])

class ConvertBooleanCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None
        return BooleanFilter(*tuple(self.args[1:])).filter(self.args[0])

class ConvertArrayCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None
        return ArrayFilter(*tuple(self.args[1:])).filter(self.args[0])

class ConvertMapCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None
        return MapFilter(*tuple(self.args[1:])).filter(self.args[0])

class ConvertObjectIdCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None
        return ObjectIdFilter(*tuple(self.args[1:])).filter(self.args[0])

class ConvertUUIDCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None
        return UUIDFilter(*tuple(self.args[1:])).filter(self.args[0])

class ConvertDateTimeCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None
        return DateTimeFilter(*tuple(self.args[1:])).filter(self.args[0])

class ConvertDateCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None
        return DateFilter(*tuple(self.args[1:])).filter(self.args[0])

class ConvertTimeCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None
        return TimeFilter(*tuple(self.args[1:])).filter(self.args[0])