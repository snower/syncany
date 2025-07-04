# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

import copy
import datetime
import json
from ..utils import human_repr_object
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase, DatabaseFactory


class ElasticsearchQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(ElasticsearchQueryBuilder, self).__init__(*args, **kwargs)

        self.orders = {}
        self.index_name = "".join(self.name.split(".")[1:])
        self.equery = None

    def filter_gt(self, key, value):
        self.query.append((key, "gt", value))

    def filter_gte(self, key, value):
        self.query.append((key, "gte", value))

    def filter_lt(self, key, value):
        self.query.append((key, "lt", value))

    def filter_lte(self, key, value):
        self.query.append((key, "lte", value))

    def filter_eq(self, key, value):
        self.query.append((key, "eq", value))

    def filter_ne(self, key, value):
        self.query.append((key, "ne", value))

    def filter_in(self, key, value):
        self.query.append((key, "in", value))

    def filter_limit(self, count, start=None):
        if not start:
            self.limit = (0, count)
        else:
            self.limit = (start, count)

    def filter_cursor(self, last_data, offset, count, primary_orders=None):
        self.limit = (offset, offset + count)

    def order_by(self, key, direct=1):
        if key in {"_index", "_type", "_id", "_score", "_source"}:
            return
        self.orders[key] = {"order": "asc"} if direct > 0 else {"order": "desc"}

    def format_table(self):
        for virtual_view in self.db.virtual_views:
            if virtual_view["name"] != self.index_name:
                continue
            if isinstance(virtual_view["query"], list):
                virtual_view["query"] = " ".join(virtual_view["query"])
            return virtual_view['query'], virtual_view.get("args", [])
        return None, None

    def format_value(self, value):
        if isinstance(value, str):
            return '"' + value + '"'
        if isinstance(value, datetime.datetime):
            return value.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        if isinstance(value, datetime.date):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, list):
            return "[" + ", ".join([self.format_value(v) for v in value]) + "]"
        if isinstance(value, dict):
            return "{" + ", ".join([self.format_value(k) + ": " + self.format_value(v) for k, v in value.items()]) + "}"
        if value is True:
            return 'true'
        if value is False:
            return 'false'
        if value is None:
            return 'null'
        return str(value)

    def format_query(self, query, virtual_query, virtual_args):
        if not isinstance(virtual_query, str):
            if "query" in virtual_query:
                if not query:
                    query = {"bool": {"must": []}}
                query["bool"]["must"].append(virtual_query["query"])
            virtual_query["query"] = query
            return virtual_query

        exps = {">": "gt", ">=": "gte", "<": "lt", "<=": "lte", "==": "eq", "!=": "ne", "in": "in"}
        virtual_values, matched_querys = [], []
        for arg in virtual_args:
            if isinstance(arg, str) or arg[1] == "==":
                arg_query = [(k, q, v) for k, q, v in self.query if arg == k and q == "eq"]
                if arg_query:
                    virtual_values.append(self.format_value(arg_query[0]))
                    matched_querys.append((arg[0], 'eq'))
                else:
                    virtual_values.append('""')
            else:
                arg_query = [(k, q, v) for k, q, v in self.query if arg[0] == k and arg[1] in exps and q == exps[arg[1]]]
                if arg_query:
                    virtual_values.append(self.format_value(arg_query[0]))
                    matched_querys.append((arg[0], arg[1]))
                else:
                    virtual_values.append('""')
        if virtual_values:
            virtual_query = virtual_query % tuple(virtual_values)

        for mq in matched_querys:
            if isinstance(mq, tuple):
                self.query = [(k, q, v) for k, q, v in self.query if mq[0] != k or q != exps[mq[1]]]
            else:
                self.query = [(k, q, v) for k, q, v in self.query if mq != k]

        virtual_query = json.loads(virtual_query)
        if not isinstance(virtual_query, str):
            if "query" in virtual_query:
                if not query:
                    query = {"bool": {"must": []}}
                query["bool"]["must"].append(virtual_query["query"])
            virtual_query["query"] = query
            return virtual_query

    def build_query(self):
        if not self.query:
            return {}

        query = {"bool": {"must": []}}
        for k, q, v in self.query:
            if q == "eq":
                query["bool"]["must"].append({"term": {k: v}})
            elif q == "in":
                query["bool"]["must"].append({"terms": {k: v}})
            elif q == "ne":
                if "must_not" not in query:
                    query["bool"]["must_not"] = []
                query["bool"]["must_not"].append({"term": {k: v}})
            else:
                query["bool"]["must"].append({"range": {k: v}})
        return query

    def commit(self):
        self.equery = self.build_query()
        virtual_query, virtual_args = self.format_table()
        if virtual_query:
            self.equery = self.format_query(self.equery, virtual_query, virtual_args)
        else:
            self.equery = {"query": self.equery}
            if self.fields:
                esfields = {"_index", "_type", "_id", "_score", "_source"}
                fields = [field[8:] for field in self.fields if field[:8] == "_source." and field not in esfields]
                if fields:
                    self.equery["_source"] = self.fields

        if not self.equery["query"]:
            self.equery.pop("query", None)
        if self.limit:
            if self.limit[0]:
                self.equery["from"] = self.limit[0]
            self.equery["size"] = self.limit[1]
        if self.orders:
            self.equery["sort"] = self.orders

        connection = self.db.ensure_connection()
        result = connection.search(self.equery, self.index_name)
        if "aggregations" in result:
            for k, v in result["aggregations"]:
                if isinstance(v, dict) and "buckets" in v:
                    return v["buckets"]
            return []
        if "hits" in result and "hits" in result["hits"]:
            return result["hits"]["hits"]
        return []

    def verbose(self):
        return "indexName: %s\nquery: %s" % (self.index_name, human_repr_object(self.equery))


class ElasticsearchInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(ElasticsearchInsertBuilder, self).__init__(*args, **kwargs)

        self.index_name = "".join(self.name.split(".")[1:])
        if isinstance(self.datas, dict):
            self.datas = [self.datas]
        self.sql = None

    def get_fields(self):
        fields = None
        for data in self.datas:
            if fields is None:
                fields = set(data.keys())
            else:
                fields = fields & set(data.keys())
        return tuple(fields) if fields else tuple()

    def get_data_primary_key(self, data):
        if len(self.primary_keys) == 1:
            return data.get(self.primary_keys[0], '')
        return ".".join([data.get(pk, '') for pk in self.primary_keys])

    def commit(self):
        fields = self.get_fields()
        datas = []
        for data in self.datas:
            datas.append({
                "_index": self.index_name,
                "_type": self.index_name,
                "_id": self.get_data_primary_key(data),
                "_source": {field: data[field] for field in fields}
            })

        connection = self.db.ensure_connection()
        return self.db.helpers().bulk(connection, datas, raise_on_exception=False, raise_on_error=False)

    def verbose(self):
        datas = ",\n    ".join([human_repr_object(value) for value in self.datas])
        return "datas(%d): \n[\n    %s\n]" % (len(self.datas), datas)


class ElasticsearchUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(ElasticsearchUpdateBuilder, self).__init__(*args, **kwargs)

        self.index_name = "".join(self.name.split(".")[1:])

    def filter_eq(self, key, value):
        self.query.append((key, "eq", value))

    def filter_in(self, key, value):
        self.query.append((key, "in", value))

    def get_fields(self, datas):
        fields = None
        for data in datas:
            if fields is None:
                fields = set(data.keys())
            else:
                fields = fields & set(data.keys())
        return tuple(fields) if fields else tuple()

    def get_data_primary_key(self, data):
        if len(self.primary_keys) == 1:
            return data.get(self.primary_keys[0], '')
        return ".".join([data.get(pk, '') for pk in self.primary_keys])

    def commit(self):
        update_datas, datas = self.expand_query_update(), []
        fields = self.get_fields(update_datas)
        for update_data in update_datas:
            datas.append({
                "_index": self.index_name,
                "_type": self.index_name,
                "_id": self.get_data_primary_key(update_data),
                "_source": {field: update_data[field] for field in fields}
            })

        connection = self.db.ensure_connection()
        return self.db.helpers().bulk(connection, datas, raise_on_exception=False, raise_on_error=False)

    def expand_query_update(self):
        if not self.primary_keys or not self.query:
            raise ValueError("Update condition error")
        update_datas = [copy.copy(self.update)]
        for key, exp, value in self.query:
            if exp == "eq" and key in self.primary_keys:
                for update_data in update_datas:
                    update_data[key] = value
            elif exp == "in" and key in self.primary_keys:
                expand_update_datas = []
                for update_data in update_datas:
                    for qvalue in value:
                        expand_update_data = copy.copy(update_data)
                        expand_update_data[key] = qvalue
                        expand_update_datas.append(expand_update_data)
                update_datas = expand_update_datas
            else:
                raise ValueError("Update condition error")
        return update_datas

    def verbose(self):
        return "filters: %s\nupdateDatas: %s" % (human_repr_object(self.query),
                                                 human_repr_object({key: self.update[key] for key in self.diff_data}))


class ElasticsearchDeleteBuilder(DeleteBuilder):
    def commit(self):
        raise NotImplementedError()


class ElasticsearchDBFactory(DatabaseFactory):
    def create(self):
        try:
            import elasticsearch
            import elasticsearch.helpers
        except ImportError:
            raise ImportError("elasticsearch>=6.3.1 is required")
        return elasticsearch.Elasticsearch(**self.config)

    def ping(self, driver):
        return True

    def close(self, driver):
        pass


class ElasticsearchDB(DataBase):
    DEFAULT_CONFIG = {
        "hosts": "127.0.0.1",
    }

    def __init__(self, manager, config):
        if "host" in config:
            config["hosts"] = config.pop("host")

        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        self.virtual_views = all_config.pop("virtual_views") if "virtual_views" in all_config else []

        super(ElasticsearchDB, self).__init__(manager, all_config)

        self.connection = None

    def build_factory(self):
        return ElasticsearchDBFactory(self.get_config_key(), self.config)

    def ensure_connection(self):
        try:
            import elasticsearch.helpers
        except ImportError:
            raise ImportError("elasticsearch>=6.3.1 is required")
        self.helpers = lambda: elasticsearch.helpers
        return self.acquire_driver().raw()

    def release_connection(self):
        self.release_driver()

    def query(self, name, primary_keys=None, fields=()):
        return ElasticsearchQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return ElasticsearchInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return ElasticsearchUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return ElasticsearchDeleteBuilder(self, name, primary_keys)

    def helpers(self):
        import elasticsearch.helpers
        return elasticsearch.helpers