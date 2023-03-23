# -*- coding: utf-8 -*-
# 2023/2/19
# create by: snower

from .calculater import Calculater
from ..filters.builtin import *

class ConvertIntCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertIntCalculater, self).__init__(*args, **kwargs)
        
        self.filter = IntFilter()
        
    def calculate(self, *args):
        if not args:
            return 0

        self.filter.args = args[1:]
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                try:
                    value = int(data)
                except:
                    value = self.filter.filter(data)
                    if value == 0:
                        continue
                result.append(value)
            return result if result else [0]
        return self.filter.filter(args[0])


class ConvertFloatCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertFloatCalculater, self).__init__(*args, **kwargs)

        self.filter = FloatFilter()
        
    def calculate(self, *args):
        if not args:
            return 0.0

        self.filter.args = args[1:]
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                try:
                    value = float(data)
                except:
                    value = self.filter.filter(data)
                    if value == 0:
                        continue
                result.append(value)
            return result if result else [0.0]
        return self.filter.filter(args[0])


class ConvertStringCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertStringCalculater, self).__init__(*args, **kwargs)

        self.filter = StringFilter()
        
    def calculate(self, *args):
        if not args:
            return ''

        self.filter.args = args[1:]
        if isinstance(args[0], list):
            return [self.filter.filter(data) for data in args[0]]
        return self.filter.filter(args[0])


class ConvertBytesCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertBytesCalculater, self).__init__(*args, **kwargs)

        self.filter = BytesFilter()
        
    def calculate(self, *args):
        if not args:
            return b''

        self.filter.args = args[1:]
        if isinstance(args[0], list):
            return [self.filter.filter(data) for data in args[0]]
        return self.filter.filter(args[0])


class ConvertBooleanCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertBooleanCalculater, self).__init__(*args, **kwargs)

        self.filter = BooleanFilter()
        
    def calculate(self, *args):
        if not args:
            return False

        self.filter.args = args[1:]
        if isinstance(args[0], list):
            return [self.filter.filter(data) for data in args[0]]
        return self.filter.filter(args[0])


class ConvertArrayCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertArrayCalculater, self).__init__(*args, **kwargs)

        self.filter = ArrayFilter()
        
    def calculate(self, *args):
        if not args:
            return []
        self.filter.args = args[1:]
        return self.filter.filter(args[0])


class ConvertMapCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertMapCalculater, self).__init__(*args, **kwargs)

        self.filter = MapFilter()
        
    def calculate(self, *args):
        if not args:
            return {}
        self.filter.args = args[1:]
        return self.filter.filter(args[0])


class ConvertObjectIdCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertObjectIdCalculater, self).__init__(*args, **kwargs)

        if ObjectId is None:
            raise ImportError(u"bson required")
        self.filter = ObjectIdFilter()
        
    def calculate(self, *args):
        default_value = ObjectId("000000000000000000000000")
        if not args:
            return default_value

        self.filter.args = args[1:]
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                try:
                    value = ObjectId(data)
                except:
                    value = self.filter.filter(data)
                    if value == default_value:
                        continue
                result.append(value)
            return result if result else [default_value]
        return self.filter.filter(args[0])


class ConvertUUIDCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertUUIDCalculater, self).__init__(*args, **kwargs)

        self.filter = UUIDFilter()
        
    def calculate(self, *args):
        default_value = uuid.UUID("00000000-0000-0000-0000-000000000000")
        if not args:
            return default_value

        self.filter.args = args[1:]
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                try:
                    value = uuid.UUID(data)
                except:
                    value = self.filter.filter(data)
                    if value == default_value:
                        continue
                result.append(value)
            return result if result else [default_value]
        return self.filter.filter(args[0])


class ConvertDateTimeCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertDateTimeCalculater, self).__init__(*args, **kwargs)

        self.filter = DateTimeFilter()
        
    def calculate(self, *args):
        if not args:
            return None

        self.filter.args = args[1:]
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                value = self.filter.filter(data)
                if value is None:
                    continue
                result.append(value)
            return result if result else [None]
        return self.filter.filter(args[0])


class ConvertDateCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertDateCalculater, self).__init__(*args, **kwargs)

        self.filter = DateFilter()
        
    def calculate(self, *args):
        if not args:
            return None

        self.filter.args = args[1:]
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                value = self.filter.filter(data)
                if value is None:
                    continue
                result.append(value)
            return result if result else [None]
        return self.filter.filter(args[0])


class ConvertTimeCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertTimeCalculater, self).__init__(*args, **kwargs)

        self.filter = TimeFilter()
        
    def calculate(self, *args):
        if not args:
            return None

        self.filter.args = args[1:]
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                value = self.filter.filter(data)
                if value is None:
                    continue
                result.append(value)
            return result if result else [None]
        return self.filter.filter(args[0])
