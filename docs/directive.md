# 指令

可以使用的指令格式说明，下文中带directive标识的都标识可支持使用新的完整指令列表。

每条指令格式下方数据作用域说明：

- A' 读取值访问当前行
- P' 读取值访问父级行，每一个 ' 往上访问一级
- C' 读取值返回当前指令的结果数据
- I' 读取值访问内部一值，如数组的每一个数据项
- G' 读取值访问全局定义值

## 常量

常量值。

```
格式1：
<value>

格式2：
["#const", <value>]
```

## 读取值

读取当前数据值。

```
格式1：
$.<name>|<filter_name>
A'

格式2：
["$.<name>|<filter_name>", ":$.<child_name>"]
  A'          C'
```

## 父级数据访问

读取上层数据值。

```
格式1：
$$.<name>|<filter_name> /* 上一层数据 /*
P'

$$$.<name>|<filter_name> /* 上两层数据 /*
P''

/* n个$符号表示读取上n-1层数据 */
```

## 联表查询

```
格式1：
["$.<name>|<filter_name>", "&.<dabase_name>.<table_name>::<primary_key>", ":<child_directive>"]
A'                                                          C'

["$.<name>|<filter_name>", ["&.<dabase_name>.<table_name>::<primary_key>", {"<condition_name": "<condition_value>"}], ":<child_directive>"]
A'                                                                                                      C'
```

## case条件分支

```
格式1：
{
    "#case": "<value_directive>",
    "<value>": "<child_directive>",
    ":<number>": "<child_directive>",
    "#end": "<child_directive>" #默认分支
}
A'
```

## 计算表达式

调用计算表达式，详细可用可看计算器一节，返回指令可选。

```
格式1：
["@<calculate_name>|<filter_name>", <args_directive...>, ":<child_directive>"]
                                    A'                    C'
```

## make重构数据

重新构造数据结构，返回指令可选。

```

格式1：
["#make", "<value_directive>", ":<child_directive>"]
          A'                  C'

格式2：
["#make", {"<key_directive": "<value_directive>"}, ":<child_directive>"]
          A'                                      C'

格式3：
["#make", "[<value_directive...>]", ":<child_directive>"]
          A'                        C'
```

## let通过值取值

读取key表达式的值，然后依据该值在当前数据读取值，返回指令可选。

```
格式1：
["#let", <key_directive>, ":<child_directive>"]
         A'                C'
```

## yield迭代器

迭代器指令，可用于展开二级数组，当二级数组为空时即跳过改行数据，返回指令可选。

```
格式1：
["#yield", <value_directive>, ":<child_directive>"]
           A'                  C'

#value表达式返回一个数组，如果有child表达式，会对数组的每一项调用child表达式。
```

## aggregate聚合计算

以key表达式的值进行分组聚合计算，其它值保留第一条数据的值。

```
格式1：
["#aggregate", <key_directive>, "<calculate_directive>"]
               A'               G'

#calculate表达式当前数据数据为以key表达式分组的全局值，当前行值为父级值，计算后值自动回写回全局值。
```

## call自定义表达式

调用全局defines里定义好的指令，相同value值不会重复计算，返回指令可选。

```
格式1：
["#call", <define_name>, <value_directive>, ":<child_directive>"]
                         A'                  C'
```

## assign全局变量赋值

读取全集variables里的值用于计算并回写回全局，例如全局序号生成，返回指令可选。

```
格式1：
["#assign", <variable_name>, <calculate_directive>, ":<child_directive>"]
                             G'                     C'

#注意：calculate表达式的输入值为全局变量值，当前行值为父级值。
```

## lambda表达式

```
格式1：
["#lambda", <directive>]
            I'
```

## foreach循环

选好value表达式返回数组，每一项输入calculate表达式返回填入新的数组，返回指令可选。

```
格式1：
["#foreach", <value_directive>, <calculate_directive>, ":<child_directive>"]
             A'                 I'                     C'
```

## break跳出循环

在foreach的calculate指令中用于跳出，如果返回指令返回有效值则保留该值。

```
格式1：
["#break", ":<child_directive>"]
           A'
```

## continue继续循环

在foreach的calculate指令中用于跳过，如果返回指令返回有效值则保留该值。

```
格式1：
["#continue", ":<child_directive>"]
              A'
```

## if条件判断

if判断分支指令，返回指令可选。

```
格式1：
["#if", <value_directive>, "<true_directive>", "<false_directive>", ":<child_directive>"]
        A'                 A'                  A'                   C'
```

## match匹配表达式

匹配指令，可以使用正则表达式匹配。

```
格式1：
{
    "#match": "<value_directive>",
    "/<regex>/": "<child_directive>",
    "#end": "<child_directive>" #默认分支
}
A'
```

## state 引用当前状态值

引用状态管理器当前状态的值。

```
格式1：
["#state", "<calcucate_directive>", "<default_directive>", ":<return_directive>"]
A'         G'                       G'                     C'
```

## cache 缓存操作

缓存操作，有缓存则加载缓存，没有则从执行加载语句，支持本地缓存和redis缓存。

```
格式1：
["#cache", "<cache_name>", "<key_directive>", "<calcucate_directive>", ":<return_directive>"]
A'                         A'                 A'                       C'
```