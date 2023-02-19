# -*- coding: utf-8 -*-
# 2023/2/19
# create by: snower

import datetime
from ..utils import get_timezone
from .calculater import Calculater

class TimeWindowCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return datetime.datetime.now(tz=get_timezone())

        time_period = self.args[0]
        dt = self.args[1] if len(self.args) >= 2 and isinstance(self.args[1], datetime.datetime) \
            else datetime.datetime.now(tz=get_timezone())
        offset = self.args[1] if len(self.args) >= 2 and isinstance(self.args[1], (int, float)) else \
            (self.args[2] if len(self.args) >= 3 and isinstance(self.args[2], (int, float)) else None)

        if time_period[-1] == "d":
            dt = datetime.datetime(dt.year, dt.month, dt.day - dt.day % int(time_period[:-1]), tzinfo=dt.tzinfo)
            if offset:
                dt = dt + datetime.timedelta(days=int(time_period[:-1]) * offset)
            return dt
        if time_period[-1] == "H":
            dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour - dt.hour % int(time_period[:-1]), tzinfo=dt.tzinfo)
            if offset:
                dt = dt + datetime.timedelta(hours=int(time_period[:-1]) * offset)
            return dt
        if time_period[-1] == "M":
            dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute - dt.minute % int(time_period[:-1]), tzinfo=dt.tzinfo)
            if offset:
                dt = dt + datetime.timedelta(minutes=int(time_period[:-1]) * offset)
            return dt
        if time_period[-1] == "S":
            dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second - dt.second % int(time_period[:-1]), tzinfo=dt.tzinfo)
            if offset:
                dt = dt + datetime.timedelta(seconds=int(time_period[:-1]) * offset)
            return dt
        return None

class DateTimeCalculater(Calculater):
    def calculate(self):
        if not self.args:
            return None

        if not isinstance(self.args[0], datetime.datetime):
            return None
        func_name = self.name[10:]
        if hasattr(self.args[0], func_name):
            return getattr(self.args[0], func_name)(*tuple(self.args[1:]))
        return None