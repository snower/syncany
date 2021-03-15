# -*- coding: utf-8 -*-
# 2021/3/15
# create by: snower

import re
import json
from .valuer import Valuer

class Matcher(object):
    def __init__(self, matcher):
        self.matcher = matcher

    def match(self, value):
        return None

class ReMatcher(Matcher):
    RE_FLAGS = {"a": re.A, "i": re.I, "l": re.L, "u": re.U, "m": re.M, "s": re.S, "x": re.X}

    def __init__(self, *args, **kwargs):
        super(ReMatcher, self).__init__(*args, **kwargs)

        index = self.matcher.rindex("/")
        flags, matcher_flags = 0, self.matcher[index + 1:].lower()
        for fn, fg in self.RE_FLAGS.items():
            if fn in matcher_flags:
                flags |= fg
        self.r = re.compile(self.matcher[1:index], flags)

    def match(self, value):
        try:
            m = self.r.match(value if isinstance(value, str) else str(value))
            if not m:
                return None
            return {"match_groups": list(m.groups()), "match": self.matcher, "value": value}
        except:
            return None

class InMatcher(Matcher):
    def __init__(self, *args, **kwargs):
        super(InMatcher, self).__init__(*args, **kwargs)

        try:
            if self.matcher[:1] == "(" and self.matcher[-1:] == ")":
                self.values = json.loads("[" + self.matcher[1:-1] + "]")
            else:
                self.values = json.loads(self.matcher)
        except:
            self.values = []

    def match(self, value):
        if isinstance(self.values, dict):
            if value not in self.values:
                return None
            return {"match_key": value, "match_value": self.values[value], "match": self.matcher, "value": value}

        for i in range(len(self.values)):
            if self.values[i] == value:
                return {"match_key": i, "match_value": self.values[i], "match": self.matcher, "value": value}
        return None

class CmpMatcher(Matcher):
    def __init__(self, *args, **kwargs):
        super(CmpMatcher, self).__init__(*args, **kwargs)

        self.cmpers = []
        for cmper in self.matcher.split(","):
            if cmper[:2] == ">=":
                self.cmpers.append(self.build_cmper(cmper[2:], lambda v, cv: v >= cv))
            elif cmper[:2] == "<=":
                self.cmpers.append(self.build_cmper(cmper[2:], lambda v, cv: v <= cv))
            elif cmper[:1] == ">":
                self.cmpers.append(self.build_cmper(cmper[1:], lambda v, cv: v > cv))
            elif cmper[:1] == "<":
                self.cmpers.append(self.build_cmper(cmper[1:], lambda v, cv: v < cv))

    def build_cmper(self, cv, cp):
        try:
            cv = json.loads(cv)
            return lambda v: cp(v, cv)
        except:
            return lambda v: False

    def match(self, value):
        for cmper in self.cmpers:
            if not cmper(value):
                return None
        return {"match": self.matcher, "value": value}

class MatchValuer(Valuer):
    def __init__(self, match_valuers, default_match_valuer, value_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.match_valuers = match_valuers
        self.default_match_valuer = default_match_valuer
        self.value_valuer = value_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        self.matched_value = None
        super(MatchValuer, self).__init__(*args, **kwargs)

    def init_valuer(self):
        self.value_wait_loaded = True if self.value_valuer and self.value_valuer.require_loaded() else False
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False
        self.matchers = []

        array_seps = {"{": "}", "[": "]", "(": ")"}
        for matcher, valuer in self.match_valuers.items():
            if not isinstance(matcher, str):
                continue
            if matcher[:1] == "/":
                if matcher.rindex("/") == 0:
                    continue
                self.matchers.append(ReMatcher(matcher))
            elif matcher[:1] in ("{", "[", "("):
                if matcher[-1:] != array_seps[matcher[0]]:
                    continue
                self.matchers.append(InMatcher(matcher))
            elif matcher[:1] in (">", "<") or matcher[:2] in ("<=", ">="):
                self.matchers.append(CmpMatcher(matcher))

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def clone(self):
        case_valuers = {}
        for name, valuer in self.match_valuers.items():
            case_valuers[name] = valuer.clone()
        default_case_valuer = self.default_match_valuer.clone() if self.default_match_valuer else None
        value_valuer = self.value_valuer.clone() if self.value_valuer else None
        return_valuer = self.return_valuer.clone() if self.return_valuer else None
        inherit_valuers = [inherit_valuer.clone() for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        return self.__class__(case_valuers, default_case_valuer, value_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, value_wait_loaded=self.value_wait_loaded, wait_loaded=self.wait_loaded,
                              matchers=self.matchers)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.value_valuer:
            self.value_valuer.fill(data)
            if self.value_wait_loaded:
                if self.default_match_valuer:
                    self.default_match_valuer.fill(data)
                return self
            self.value = self.value_valuer.get()
        else:
            self.value = data

        for matcher in self.matchers:
            self.matched_value = matcher.match(self.value)
            if self.matched_value is not None:
                self.match_valuers[matcher.matcher].fill(self.matched_value)
                break
        if self.matched_value is None and self.default_match_valuer:
            self.default_match_valuer.fill(data)

        if self.wait_loaded:
            if self.matched_value is not None:
                self.do_filter(self.match_valuers[self.matched_value["match"]].get())
            elif self.default_match_valuer:
                self.do_filter(self.default_match_valuer.get())
            self.return_valuer.fill(self.value)
        return self

    def get(self):
        if self.value_valuer and self.value_wait_loaded:
            self.value = self.value_valuer.get()
        elif self.wait_loaded:
            return self.return_valuer.get()

        if self.matched_value is not None:
            self.do_filter(self.match_valuers[self.matched_value["match"]].get())
        elif self.default_match_valuer:
            self.do_filter(self.default_match_valuer.get())

        if self.return_valuer:
            self.return_valuer.fill(self.value)
            self.value = self.return_valuer.get()
        return self.value

    def childs(self):
        childs = list(self.match_valuers.values())
        if self.default_match_valuer:
            childs.append(self.default_match_valuer)
        if self.value_valuer:
            childs.append(self.value_valuer)
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        fields = self.value_valuer.get_fields() if self.value_valuer else [self.key]
        for _, valuer in self.match_valuers.items():
            for field in valuer.get_fields():
                fields.append(field)

        if self.default_match_valuer:
            for field in self.default_match_valuer.get_fields():
                fields.append(field)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        if self.return_valuer:
            return self.return_valuer.get_final_filter()

        if self.filter:
            return self.filter

        final_filter = None
        for _, valuer in self.match_valuers.items():
            filter = valuer.get_final_filter()
            if filter is None:
                continue

            if final_filter is not None and final_filter.__class__ != filter.__class__:
                return None
            final_filter = filter

        if self.default_match_valuer:
            filter = self.default_match_valuer.get_final_filter()
            if filter is None:
                return final_filter

            if final_filter is not None and final_filter.__class__ != filter.__class__:
                return None
        return final_filter