# -*- coding: utf-8 -*-
# 2021/11/17
# create by: snower

import json
from urllib.parse import urlencode
from urllib3 import encode_multipart_formdata
from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase


class HttpRequestError(Exception):
    pass


class HttpQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(HttpQueryBuilder, self).__init__(*args, **kwargs)

        self.query_queues = {}
        db_name = self.name.split(".")
        if len(db_name) > 1:
            self.view_name = ".".join(db_name[1:])
        else:
            self.view_name = db_name[0]
        self.response = None
        self.response_urls = []

    def filter_gt(self, key, value):
        self.query[key + "__gt"] = str(value)

    def filter_gte(self, key, value):
        self.query[key + "__gte"] = str(value)

    def filter_lt(self, key, value):
        self.query[key + "__lt"] = str(value)

    def filter_lte(self, key, value):
        self.query[key + "__lte"] = str(value)

    def filter_eq(self, key, value):
        self.query[key] = str(value)

    def filter_ne(self, key, value):
        self.query[key + "__ne"] = str(value)

    def filter_in(self, key, value):
        self.query_queues[key] = value

    def filter_limit(self, count, start=None):
        self.query["limitStart"] = str(start or 0)
        self.query["limitCount"] = str(count)

    def filter_cursor(self, last_data, offset, count):
        self.query["limitStart"] = str(offset or 0)
        self.query["limitCount"] = str(count)

    def order_by(self, key, direct=1):
        self.query["orderBy"] = str(key)
        self.query["orderDirection"] = "ASC" if direct > 0 else "DESC"

    def commit(self):
        if self.view_name not in self.db.views:
            return []
        view = self.db.views[self.view_name]
        if "query" not in view["action"]:
            return []

        if not self.query_queues:
            data, self.response = self.db.request(view, view["action"]["query"], self.query, {})
            self.response_urls.append(self.response.request.url)
            if isinstance(data, list):
                return data
            return [data]

        count = max([len(values) for _, values in self.query_queues.items()])
        datas = []
        for i in range(count):
            query = dict(**self.query)
            for key, values in self.query_queues.items():
                if i >= len(values):
                    continue
                query[key] = values[i]
            data, self.response = self.db.request(view, view["action"]["query"], self.query, {})
            self.response_urls.append(self.response.request.url)
            if isinstance(data, list):
                datas.extend(data)
            else:
                datas.extend(data)
        return datas

    def verbose(self):
        if not self.response:
            return ""
        return "request: %s %s %s\nresponse: %s %s\nbody:\n%s" % (
            self.response.request.method, self.response_urls if len(self.response_urls) > 1 else self.response_urls[0],
            self.response.headers, self.response.status_code, self.response.headers, self.response.text)


class HttpInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(HttpInsertBuilder, self).__init__(*args, **kwargs)

        db_name = self.name.split(".")
        if len(db_name) > 1:
            self.view_name = ".".join(db_name[1:])
        else:
            self.view_name = db_name[0]
        self.response = None
        self.response_urls = []

    def commit(self):
        if self.view_name not in self.db.views:
            raise HttpRequestError("unknown action")
        view = self.db.views[self.view_name]
        if "query" not in view["action"]:
            raise HttpRequestError("unsupport action")

        if "batch_create" in view and view["batch_create"]:
            _, self.response = self.db.request(view, view["action"]["create"], {}, self.datas)
            self.response_urls.append(self.response.request.url)
            return

        for data in self.datas:
            _, self.response = self.db.request(view, view["action"]["create"], {}, data)
            self.response_urls.append(self.response.request.url)

    def verbose(self):
        if not self.response:
            return ""
        return "request: %s %s %s\nresponse: %s %s\nbody:\n%s" % (
            self.response.request.method, self.response_urls if len(self.response_urls) > 1 else self.response_urls[0],
            self.response.headers, self.response.status_code, self.response.headers, self.response.text)


class HttpUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(HttpUpdateBuilder, self).__init__(*args, **kwargs)

        db_name = self.name.split(".")
        if len(db_name) > 1:
            self.view_name = ".".join(db_name[1:])
        else:
            self.view_name = db_name[0]
        self.response = None

    def filter_gt(self, key, value):
        self.query[key + "__gt"] = str(value)

    def filter_gte(self, key, value):
        self.query[key + "__gte"] = str(value)

    def filter_lt(self, key, value):
        self.query[key + "__lt"] = str(value)

    def filter_lte(self, key, value):
        self.query[key + "__lte"] = str(value)

    def filter_eq(self, key, value):
        self.query[key] = str(value)

    def filter_ne(self, key, value):
        self.query[key + "__ne"] = str(value)

    def filter_in(self, key, value):
        self.query[key] = value

    def commit(self):
        if self.view_name not in self.db.views:
            raise HttpRequestError("unknown action")
        view = self.db.views[self.view_name]
        if "query" not in view["action"]:
            raise HttpRequestError("unsupport action")

        _, self.response = self.db.request(view, view["action"]["update"], self.query, self.diff_data)

    def verbose(self):
        if not self.response:
            return ""
        return "request: %s %s %s\nresponse: %s %s\nbody:\n%s" % (
            self.response.request.method, self.response.request.url, self.response.headers,
            self.response.status_code, self.response.headers, self.response.text)


class HttpDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(HttpDeleteBuilder, self).__init__(*args, **kwargs)

        db_name = self.name.split(".")
        if len(db_name) > 1:
            self.view_name = ".".join(db_name[1:])
        else:
            self.view_name = db_name[0]
        self.response = None

    def filter_gt(self, key, value):
        self.query[key + "__gt"] = str(value)

    def filter_gte(self, key, value):
        self.query[key + "__gte"] = str(value)

    def filter_lt(self, key, value):
        self.query[key + "__lt"] = str(value)

    def filter_lte(self, key, value):
        self.query[key + "__lte"] = str(value)

    def filter_eq(self, key, value):
        self.query[key] = str(value)

    def filter_ne(self, key, value):
        self.query[key + "__ne"] = str(value)

    def filter_in(self, key, value):
        self.query[key] = value

    def commit(self):
        if self.view_name not in self.db.views:
            raise HttpRequestError("unknown action")
        view = self.db.views[self.view_name]
        if "query" not in view["action"]:
            raise HttpRequestError("unsupport action")

        _, self.response = self.db.request(view, view["action"]["delete"], self.query, {})

    def verbose(self):
        if not self.response:
            return ""
        return "request: %s %s %s\nresponse: %s %s\nbody:\n%s" % (
            self.response.request.method, self.response.request.url, self.response.headers,
            self.response.status_code, self.response.headers, self.response.text)


class HttpDataBase(DataBase):
    DEFAULT_CONFIG = {
        "base_url": "http://localhost",
        "headers": {},
        "timeout": 60,
        "verify": None,
        "cert": None,
        "proxies": None,
        "auth": None,
        "checker": {
            "status_code": 200,
            # "body": {
            #     "err_code": 0
            # }
            # "return": "data"
        },
        "virtual_views": [
            # {
            #     "name": "",
            #     "action": {
            #         "query": "get",
            #         "create": "post",
            #         "update": "put",
            #         "delete": "delete"
            #     },
            #     "path": "/",
            #     "headers": {},
            #     "timeout": 60,
            #     "verify": None,
            #     "cert": None,
            #     "proxies": None,
            #     "auth": None,
            #     "checker": {
            #         "status_code": 200,
            #         # "body": {
            #         #     "err_code": 0
            #         # }
            #         # "return": "data"
            #     },
            #     "batch_create": False,
            # }
        ],
    }

    def __init__(self, manager, config):
        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)
        self.views = {v["name"]: v for v in config.pop("virtual_views")} if "virtual_views" else {}

        super(HttpDataBase, self).__init__(manager, all_config)
        self.session = None

    def ensure_session(self):
        if not self.session:
            try:
                import requests
            except ImportError:
                raise ImportError("requests>=2.22.0 is required")
            self.session = requests.session()
        return self.session

    def get_base_params(self, view):
        params = {
            "headers": {key.lower(): value for key, value in self.config["headers"].items()}
        }
        for key, value in view.get("headers", {}).items():
            params["headers"][key.lower()] = value

        for key in ("timeout", "verify", "cert", "proxies", "auth"):
            value = view[key] if key in view else self.config[key]
            if isinstance(value, list):
                value = tuple(value)
            params[key] = value
        return params

    def request(self, view, method, query, data):
        session = self.ensure_session()
        url = self.config["base_url"] + view["path"]
        params = self.get_base_params(view)
        if query:
            for key, value in list(query.items()):
                ukey = "{{%s}}" % key
                if ukey not in url:
                    continue
                url = url.replace(ukey, value)
                query.pop(key)
        if method in ("get", "delete"):
            query_string = urlencode(dict(**query, **data))
            if "?" in url:
                url, data = url + "&" + query_string, None
            else:
                url, data = url + "?" + query_string, None
        else:
            if "?" in url:
                url += ("?" + urlencode(query)) if query else ""
            else:
                url += ("&" + urlencode(query)) if query else ""
            if "content-type" in params["headers"]:
                if "json" in params["headers"]["content-type"]:
                    data = json.dumps(data, default=str, ensure_ascii=False).encode("utf-8")
                elif "x-www-form-urlencoded" in params["headers"]["content-type"]:
                    data = urlencode(data)
            if isinstance(data, dict):
                data, content_type = encode_multipart_formdata(data)
                params["headers"]["content-type"] = content_type

        response = session.request(method, url, data=data, **params)
        checker = view["checker"] if "checker" in view else self.config["checker"]
        if response.status_code != checker.get("status_code", 200):
            raise HttpRequestError(response.reason)
        if "body" in checker and isinstance(checker["body"], str):
            if checker["body"] not in response.text:
                raise HttpRequestError(response.text)
            return response.text, response

        try:
            body = json.loads(response.content)
        except Exception as e:
            raise HttpRequestError(response.text)
        if "body" not in checker:
            if "return" in checker and checker["return"]:
                from ..taskers.tasker import current_tasker
                body = current_tasker().run_valuer("$." + checker["return"], body)
            return body, response
        for key, value in checker["body"].items():
            try:
                from ..taskers.tasker import current_tasker
                bvalue = type(value)(current_tasker().run_valuer("$." + key, value))
            except Exception as e:
                raise HttpRequestError(response.text)
            if value != bvalue:
                raise HttpRequestError(response.text)
        if "return" in checker and checker["return"]:
            from ..taskers.tasker import current_tasker
            body = current_tasker().run_valuer("$." + checker["return"], body)
        return body, response

    def query(self, name, primary_keys=None, fields=()):
        return HttpQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys=None, fields=(), datas=None):
        return HttpInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys=None, fields=(), update=None, diff_data=None):
        return HttpUpdateBuilder(self, name, primary_keys, fields, update, diff_data)

    def delete(self, name, primary_keys=None):
        return HttpDeleteBuilder(self, name, primary_keys)

    def close(self):
        if not self.session:
            return
        self.session.close()
