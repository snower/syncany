# -*- coding: utf-8 -*-
# 2020/6/28
# create by: snower

import weakref
from .valuer import Valuer


class InheritValuer(Valuer):
    def __init__(self, value_valuer, *args, **kwargs):
        if isinstance(self, ContextInheritValuer):
            self.child_valuer = ContextInheritChildValuer(self, value_valuer, *args, **kwargs, contexter=self.contexter)
        else:
            self.child_valuer = InheritChildValuer(self, value_valuer, *args, **kwargs)
        self.value_valuer = value_valuer
        self.cloned_child_valuer = None
        super(InheritValuer, self).__init__(*args, **kwargs)

    def get_inherit_child_valuer(self):
        return self.child_valuer

    def clone(self, contexter=None):
        if self.child_valuer.cloned_inherit_valuer:
            inherit_valuer = self.child_valuer.cloned_inherit_valuer
            self.child_valuer.cloned_inherit_valuer = None
            return inherit_valuer

        value_valuer = self.value_valuer.clone(contexter) if self.value_valuer else None
        if contexter is not None:
            inherit_valuer = ContextInheritValuer(value_valuer, self.key, self.filter, from_valuer=self,
                                                  contexter=contexter)
        elif isinstance(self, ContextInheritValuer):
            inherit_valuer = ContextInheritValuer(value_valuer, self.key, self.filter, from_valuer=self,
                                                  contexter=self.contexter)
        else:
            inherit_valuer = self.__class__(value_valuer, self.key, self.filter, from_valuer=self)
        self.cloned_child_valuer = inherit_valuer.get_inherit_child_valuer()
        return inherit_valuer

    def fill(self, data):
        if self.value_valuer:
            self.value_valuer.fill(self.do_filter(data))
        else:
            super(InheritValuer, self).fill(data)
        return self

    def get(self):
        return None

    def childs(self):
        if not self.value_valuer:
            return []
        return [self.value_valuer]

    def get_fields(self):
        if not self.value_valuer:
            return []
        return self.value_valuer.get_fields()

    def get_final_filter(self):
        return None


class ContextInheritValuer(InheritValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        super(ContextInheritValuer, self).__init__(*args, **kwargs)
        self.value_context_id = (id(self.child_valuer), "value")

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


class InheritChildValuer(Valuer):
    def __init__(self, inherit_valuer, value_valuer, *args, **kwargs):
        self.inherit_valuer = weakref.proxy(inherit_valuer)
        self.value_valuer = value_valuer
        self.cloned_inherit_valuer = None
        super(InheritChildValuer, self).__init__(*args, **kwargs)

    def clone(self, contexter=None):
        if self.inherit_valuer.cloned_child_valuer:
            child_valuer = self.inherit_valuer.cloned_child_valuer
            self.inherit_valuer.cloned_child_valuer = None
            return child_valuer

        value_valuer = self.value_valuer.clone(contexter) if self.value_valuer else None
        if contexter is not None:
            self.cloned_inherit_valuer = ContextInheritValuer(value_valuer, self.key, self.filter, from_valuer=self,
                                                              contexter=contexter)
        elif isinstance(self, ContextInheritChildValuer):
            self.cloned_inherit_valuer = ContextInheritValuer(value_valuer, self.key, self.filter, from_valuer=self,
                                                              contexter=self.contexter)
        else:
            self.cloned_inherit_valuer = InheritValuer(value_valuer, self.key, self.filter, from_valuer=self)
        return self.cloned_inherit_valuer.get_inherit_child_valuer()

    def fill(self, data):
        return self

    def get(self):
        if self.value_valuer:
            return self.value_valuer.get()
        return self.value

    def childs(self):
        if not self.value_valuer:
            return []
        return [self.value_valuer]

    def get_fields(self):
        return []

    def get_final_filter(self):
        if not self.value_valuer:
            if self.filter:
                return self.filter
            return None
        return self.value_valuer.get_final_filter()

    def require_loaded(self):
        return False


class ContextInheritChildValuer(InheritChildValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = (id(self), "value")
        super(ContextInheritChildValuer, self).__init__(*args, **kwargs)

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
