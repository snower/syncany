# -*- coding: utf-8 -*-
# 18/8/6
# create by: snower

def get_expression_name(expression):
    return {
        "==": "eq",
        "!=": "ne",
        ">": "gt",
        ">=": "gte",
        "<": "lt",
        "<=": "lte"
    }[expression]