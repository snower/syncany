# -*- coding: utf-8 -*-
# 2020/7/31
# create by: snower

class SyncanyException(Exception):
    pass


class LoaderUnknownException(SyncanyException):
    pass


class OutputerUnknownException(SyncanyException):
    pass


class ValuerUnknownException(SyncanyException):
    pass


class FilterUnknownException(SyncanyException):
    pass


class CalculaterUnknownException(SyncanyException):
    pass


class DatabaseUnknownException(SyncanyException):
    pass

class SourceUnknownException(SyncanyException):
    pass