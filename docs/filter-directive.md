# 过滤器

## int

转换为整数。

```
null|int => 0
false|int => 0
true|int => 1
1|int => 1
2.1|int => 2
datetime|int => timestamp()
date|int => timestamp(day)
timedelta|int => total_seconds()
array<?> => sum(foreach(item => int(item)))
map<?, ?> => sum(foreach(key, value => int(value)))
object => int(object)
? => 0
```

## float

转换为浮点数。

```
null|int => 0.0
false|int => 0.0
true|int => 1.0
1|int => 1.0
2.1|int => 2.1
datetime|int => timestamp()
date|int => timestamp(day)
timedelta|int => total_seconds()
array<?> => sum(foreach(item => float(item)))
map<?, ?> => sum(foreach(key, value => float(value)))
object => float(object)
? => 0.0
```

## str

格式化为字符串。

## bytes
## bool
## array
## map
## ObjectId
## datetime
## date
## time