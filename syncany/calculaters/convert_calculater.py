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

        self.filter.args = args[1] if len(args) >= 2 else None
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                if data is None:
                    continue
                try:
                    value = int(data)
                except:
                    value = self.filter.filter(data)
                result.append(value)
            return result if result else [0]
        if args[0] is None:
            return 0
        return self.filter.filter(args[0])

    def get_final_filter(self):
        return self.filter


class ConvertFloatCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertFloatCalculater, self).__init__(*args, **kwargs)

        self.filter = FloatFilter()
        
    def calculate(self, *args):
        if not args:
            return 0.0

        self.filter.args = args[1] if len(args) >= 2 else None
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                if data is None:
                    continue
                try:
                    value = float(data)
                except:
                    value = self.filter.filter(data)
                result.append(value)
            return result if result else [0.0]
        if args[0] is None:
            return 0.0
        return self.filter.filter(args[0])

    def get_final_filter(self):
        return self.filter


class ConvertDecimalCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertDecimalCalculater, self).__init__(*args, **kwargs)

        self.filter = DecimalFilter()

    def calculate(self, *args):
        if not args:
            return Decimal(0.0)

        self.filter.args = args[1] if len(args) >= 2 else None
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                if data is None:
                    continue
                try:
                    value = Decimal(data)
                except:
                    value = self.filter.filter(data)
                result.append(value)
            return result if result else [Decimal(0.0)]
        if args[0] is None:
            return Decimal(0.0)
        return self.filter.filter(args[0])

    def get_final_filter(self):
        return self.filter


class ConvertStringCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertStringCalculater, self).__init__(*args, **kwargs)

        self.filter = StringFilter()
        
    def calculate(self, *args):
        if not args:
            return ''

        self.filter.args = args[1] if len(args) >= 2 else None
        if isinstance(args[0], list):
            return [self.filter.filter(data) for data in args[0] if data is not None]
        if args[0] is None:
            return ''
        return self.filter.filter(args[0])

    def get_final_filter(self):
        return self.filter


class ConvertBytesCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertBytesCalculater, self).__init__(*args, **kwargs)

        self.filter = BytesFilter()
        
    def calculate(self, *args):
        if not args:
            return b''

        self.filter.args = args[1] if len(args) >= 2 else None
        if isinstance(args[0], list):
            return [self.filter.filter(data) for data in args[0] if data is not None]
        if args[0] is None:
            return b''
        return self.filter.filter(args[0])

    def get_final_filter(self):
        return self.filter


class ConvertBooleanCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertBooleanCalculater, self).__init__(*args, **kwargs)

        self.filter = BooleanFilter()
        
    def calculate(self, *args):
        if not args:
            return False

        self.filter.args = args[1] if len(args) >= 2 else None
        if isinstance(args[0], list):
            return [self.filter.filter(data) for data in args[0] if data is not None]
        if args[0] is None:
            return False
        return self.filter.filter(args[0])

    def get_final_filter(self):
        return self.filter


class ConvertArrayCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertArrayCalculater, self).__init__(*args, **kwargs)

        self.filter = ArrayFilter()
        
    def calculate(self, *args):
        if not args:
            return []
        self.filter.args = args[1] if len(args) >= 2 else None
        if args[0] is None:
            return []
        return self.filter.filter(args[0])

    def get_final_filter(self):
        return self.filter


class ConvertSetCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertSetCalculater, self).__init__(*args, **kwargs)

        self.filter = SetFilter()

    def calculate(self, *args):
        if not args:
            return set([])
        self.filter.args = args[1] if len(args) >= 2 else None
        if args[0] is None:
            return set([])
        return self.filter.filter(args[0])

    def get_final_filter(self):
        return self.filter


class ConvertMapCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertMapCalculater, self).__init__(*args, **kwargs)

        self.filter = MapFilter()
        
    def calculate(self, *args):
        if not args:
            return {}
        self.filter.args = args[1] if len(args) >= 2 else None
        if args[0] is None:
            return {}
        return self.filter.filter(args[0])

    def get_final_filter(self):
        return self.filter


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

        self.filter.args = args[1] if len(args) >= 2 else None
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                if data is None:
                    continue
                try:
                    value = ObjectId(data)
                except:
                    value = self.filter.filter(data)
                result.append(value)
            return result if result else [default_value]
        if args[0] is None:
            return default_value
        return self.filter.filter(args[0])

    def get_final_filter(self):
        return self.filter


class ConvertUUIDCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertUUIDCalculater, self).__init__(*args, **kwargs)

        self.filter = UUIDFilter()
        
    def calculate(self, *args):
        default_value = uuid.UUID("00000000-0000-0000-0000-000000000000")
        if not args:
            return default_value

        self.filter.args = args[1] if len(args) >= 2 else None
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                if data is None:
                    continue
                try:
                    value = uuid.UUID(data)
                except:
                    value = self.filter.filter(data)
                result.append(value)
            return result if result else [default_value]
        if args[0] is None:
            return default_value
        return self.filter.filter(args[0])

    def get_final_filter(self):
        return self.filter


class ConvertDateTimeCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertDateTimeCalculater, self).__init__(*args, **kwargs)

        self.filter = DateTimeFilter()
        
    def calculate(self, *args):
        if not args:
            return None

        self.filter.args = args[1] if len(args) >= 2 else None
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                if data is None:
                    continue
                value = self.filter.filter(data)
                result.append(value)
            return result if result else [None]
        if args[0] is None:
            return None
        return self.filter.filter(args[0])

    def get_final_filter(self):
        return self.filter


class ConvertDateCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertDateCalculater, self).__init__(*args, **kwargs)

        self.filter = DateFilter()
        
    def calculate(self, *args):
        if not args:
            return None

        self.filter.args = args[1] if len(args) >= 2 else None
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                if data is None:
                    continue
                value = self.filter.filter(data)
                result.append(value)
            return result if result else [None]
        if args[0] is None:
            return None
        return self.filter.filter(args[0])

    def get_final_filter(self):
        return self.filter


class ConvertTimeCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(ConvertTimeCalculater, self).__init__(*args, **kwargs)

        self.filter = TimeFilter()
        
    def calculate(self, *args):
        if not args:
            return None

        self.filter.args = args[1] if len(args) >= 2 else None
        if isinstance(args[0], list):
            result = []
            for data in args[0]:
                if data is None:
                    continue
                value = self.filter.filter(data)
                result.append(value)
            return result if result else [None]
        if args[0] is None:
            return None
        return self.filter.filter(args[0])

    def get_final_filter(self):
        return self.filter
