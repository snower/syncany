# -*- coding: utf-8 -*-
# 2020/7/31
# create by: snower

import logging

__logger = logging


def get_logger():
    return __logger


def set_logger(logger):
    global __logger
    __logger = logger