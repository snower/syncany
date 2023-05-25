# -*- coding: utf-8 -*-
# 2021/3/15
# create by: snower

import re
import json
from .valuer import Valuer


class Matcher(object):
    ARRAY_SEPS = {"{": "}", "[": "]", "(": ")"}

    def __init__(self, matcher, valuer):
        self.matcher = matcher
        self.valuer = valuer

    @staticmethod
    def compile(matcher, valuer):
        if not isinstance(matcher, str):
            return None
        if matcher[:1] == "/":
            if matcher.rindex("/") == 0:
                return None
            return ReMatcher(matcher, valuer)
        elif matcher[:1] in ("{", "[", "("):
            if matcher[-1:] != Matcher.ARRAY_SEPS[matcher[0]]:
                return None
            return InMatcher(matcher, valuer)
        elif matcher[:1] in (">", "<") or matcher[:2] in ("<=", ">="):
            return CmpMatcher(matcher, valuer)
        return None

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
                for i in range(len(self.values)):
                    matcher = Matcher.compile(self.values[i], self.valuer)
                    if matcher is not None:
                        self.values[i] = matcher
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
            if isinstance(self.values[i], Matcher):
                match = self.values[i].match(value)
                if match is None:
                    continue
                return match
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
    matched_value = None

    def __init__(self, match_valuers, default_match_valuer, value_valuer, return_valuer, inherit_valuers, *args, **kwargs):
        self.match_valuers = match_valuers
        self.default_match_valuer = default_match_valuer
        self.value_valuer = value_valuer
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(MatchValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(MatchValuer, self).new_init()
        self.value_wait_loaded = True if self.value_valuer and self.value_valuer.require_loaded() else False
        self.match_wait_loaded = self.check_wait_loaded()
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False
        self.matchers = []

        for matcher, valuer in self.match_valuers.items():
            matcher = Matcher.compile(matcher, valuer)
            if matcher is None:
                continue
            self.matchers.append(matcher)

    def clone_init(self, from_valuer):
        super(MatchValuer, self).clone_init(from_valuer)
        self.value_wait_loaded = from_valuer.value_wait_loaded
        self.match_wait_loaded = from_valuer.match_wait_loaded
        self.wait_loaded = from_valuer.wait_loaded
        self.matchers = from_valuer.matchers

    def check_wait_loaded(self):
        for name, valuer in self.match_valuers.items():
            if valuer.require_loaded():
                return True
        if self.default_match_valuer and self.default_match_valuer.require_loaded():
            return True
        return False

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_loader(self, is_return_getter=True, **kwargs):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_loader(is_return_getter=False, **kwargs)
        for name, valuer in self.match_valuers.items():
            valuer.mount_loader(is_return_getter=False, **kwargs)
        if self.default_match_valuer:
            self.default_match_valuer.mount_loader(is_return_getter=False, **kwargs)
        if self.value_valuer:
            self.value_valuer.mount_loader(is_return_getter=False, **kwargs)
        if self.return_valuer:
            self.return_valuer.mount_loader(is_return_getter=is_return_getter and True, **kwargs)

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        match_valuers = {}
        for name, valuer in self.match_valuers.items():
            match_valuers[name] = valuer.clone(contexter, **kwargs)
        default_match_valuer = self.default_match_valuer.clone(contexter, **kwargs) \
            if self.default_match_valuer else None
        value_valuer = self.value_valuer.clone(contexter, **kwargs) if self.value_valuer else None
        return_valuer = self.return_valuer.clone(contexter, **kwargs) if self.return_valuer else None
        if contexter is not None:
            return ContextMatchValuer(match_valuers, default_match_valuer, value_valuer, return_valuer, inherit_valuers,
                                      self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextMatchValuer):
            return ContextMatchValuer(match_valuers, default_match_valuer, value_valuer, return_valuer, inherit_valuers,
                                      self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(match_valuers, default_match_valuer, value_valuer, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)
    
    def reinit(self):
        self.matched_value = None
        return super(MatchValuer, self).reinit()

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.value_valuer:
            if self.value_wait_loaded:
                self.value_valuer.fill(data)
                if self.default_match_valuer:
                    self.default_match_valuer.fill(data)
                return self
            value = self.value_valuer.fill_get(data)
        else:
            value = data

        matched_value = None
        for matcher in self.matchers:
            matched_value = matcher.match(value)
            if matched_value is not None:
                self.match_valuers[matcher.matcher].fill(matched_value)
                break
        if matched_value is None and self.default_match_valuer:
            self.default_match_valuer.fill(data)

        if not self.match_wait_loaded or self.wait_loaded:
            if matched_value is not None:
                value = self.do_filter(self.match_valuers[matched_value["match"]].get())
            elif self.default_match_valuer:
                value = self.do_filter(self.default_match_valuer.get())
            else:
                value = self.do_filter(None)
            if self.return_valuer:
                if not self.wait_loaded:
                    self.value = self.return_valuer.fill_get(value)
                else:
                    self.return_valuer.fill(value)
            else:
                self.value = value
        self.matched_value = matched_value
        return self

    def get(self):
        if self.value_valuer and self.value_wait_loaded:
            value, matched_value = self.value_valuer.get(), None
            for matcher in self.matchers:
                matched_value = matcher.match(value)
                if matched_value is not None:
                    self.match_valuers[matcher.matcher].fill(matched_value)
                    break
        elif not self.match_wait_loaded or self.wait_loaded:
            if self.return_valuer:
                if not self.wait_loaded:
                    return self.value
                return self.return_valuer.get()
            return self.value
        else:
            value, matched_value = self.value, self.matched_value

        if matched_value is not None:
            value = self.do_filter(self.match_valuers[matched_value["match"]].get())
        elif self.default_match_valuer:
            value = self.do_filter(self.default_match_valuer.get())
        else:
            value = self.do_filter(None)

        if self.return_valuer:
            return self.return_valuer.fill_get(value)
        return value

    def fill_get(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        value = self.value_valuer.fill_get(data) if self.value_valuer else data
        for matcher in self.matchers:
            matched_value = matcher.match(value)
            if matched_value is not None:
                return self.match_valuers[matcher.matcher].fill_get(matched_value)
        if self.default_match_valuer:
            return self.default_match_valuer.fill_get(data)
        return self.do_filter(None)

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


class ContextMatchValuer(MatchValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        self.matched_value_context_id = (id(self), "matched_value")
        super(ContextMatchValuer, self).__init__(*args, **kwargs)

    @property
    def value(self):
        try:
            return self.contexter.values[self.value_context_id]
        except KeyError:
            return None

    @value.setter
    def value(self, v):
        if v is None:
            if self.value_context_id in self.contexter.values:
                self.contexter.values.pop(self.value_context_id)
            return
        self.contexter.values[self.value_context_id] = v

    @property
    def matched_value(self):
        try:
            return self.contexter.values[self.matched_value_context_id]
        except KeyError:
            return None

    @matched_value.setter
    def matched_value(self, v):
        if v is None:
            if self.matched_value_context_id in self.contexter.values:
                self.contexter.values.pop(self.matched_value_context_id)
            return
        self.contexter.values[self.matched_value_context_id] = v
