# -*- coding: utf-8 -*-
# 2021/10/29
# create by: snower

import os
import re

ENVIRONMENT_VARIABLES_RE = re.compile(r"(\$\{(\w+?)(:.*?)?\})", re.DOTALL | re.M)

class Parser(object):
    def __init__(self, content):
        self.content = content

    def parse_environment_variables(self, content):
        environment_variables = ENVIRONMENT_VARIABLES_RE.findall(content)
        for environment_variable, environment_variable_name, default_value in environment_variables:
            if default_value:
                environment_value = os.getenv(environment_variable_name.strip())
                if environment_value is None:
                    content = content.replace(environment_variable, default_value[1:])
                else:
                    content = content.replace(environment_variable, environment_value)
            else:
                environment_value = os.getenv(environment_variable_name.strip())
                if environment_value is None:
                    content = content.replace(environment_variable, "")
                else:
                    content = content.replace(environment_variable, environment_value)
        return content

    def parse(self):
        raise NotImplementedError