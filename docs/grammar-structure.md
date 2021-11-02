# 语法

## 数据源引用

操作符：&

在配置文件中input或ouput字段指定输入输出数据源，或在schema中做join查询时指定关联数据源信息。

```
例：
input: "&.json.order.json::id" /* 从数据库json定义的目录中读入order.json文件内容 */

output: "&.json.order.json::id" /* 数据输出到数据库json定义的目录的order.json文件中 */

output: "&.stdio.&1::id" /* 控制台输出 */

schema: {
    order_fee: ["$.order_id", "&.order.order_table::order_id", ":$.order_fee"] /* 用order_id关联查询order数据库的order_table，读取order_fee /*
}
```

## 指令调用

操作符：#

```
可用格式：
#<cmd>
[#<cmd>, <arg1>, <arg2>]
[#<cmd>, <arg1>, <arg2>, :<return_cmd>]

例：
#break /* foreach指令中中断循环 */
[#if, $.a, 1, 2] /* if指令，如果a的值为真则当前值为1， 否则为2 */
[#make, {"a": 1}, ":$.a"] /*构造一个hasb对象，并且返回key为a的值 */
```

详细可用指令列表及指令使用方式请看“指令格式”段落。

## 值读取

操作符：$

读取当前对象的属性值，当前对象可以是hash对象，也可以是数组。

```
例：
$.a /* 读取a的值 */
$.a.b /* 读取a的值，然后再读取a的b的值 */
$.a.:0 /* 读取a的值，然后读取数组a的索引0值 */
$.a|int /* 读取a的值，然后尝试转换为数字类型 */
$.* /* 读取当前整个值 */
```

## 返回值

操作符：:

在指令调用中标识返回值或指令信息。

```
["$.a|int", "&.mysql.test::id", ":$.id"] /* 读取a的值关联查询mysql的test表id字段，然后返回test表的id的值 */
["$.a", ":$.:0"] /* 读取a的值，然后返回数组a的第一项值 */
["@add", 1, "$.a", [":@mul", "$.*", 4]] /* 读取a的值然后和1相加，接着再乘于4 */
["#foreach", "$.*|array", ["#make", {"a": "$.a"}], [
    ":@sum", "$.*|array", "a""
]] /* 循环当前数据然后构造一个{"a": "$.a"}的数组，最后求数组sum加和  */ 
```

## 预处理

操作符：%

在加载配置文件过程中用source配置中的相同名称的文件内容替换标啥。

## 过滤器

操作符：|

用于取值或计算表达式对结果进行格式化。

```
例：
$.a|int /* 读取a的值并格式化为数字 */
$.a.:0|string /* 读取值并格式化为字符串 */
["@add|string", 1, 2] /* 计算1+2并且把结果格式化为字符串 */
["$.a|int", "&.mysql.test::id", "$.created_at|datetime"] /* 读取a的值转化为数字后关联mysql的test表的id字段查询，然后读取查询结果的created_at并格式化为datetime */
```

详细可用指令列表及指令使用方式请看“过滤器”段落。

## 计算表达式

操作符：@

调用函数计算结果。

```
例：
["@add", 1, 2] /* 计算1+2 */
["@sum", "$.*|array", "num", [":@add", "$.*|int", 3]] /* 对数组求sum和，然后加3 */
```

## 引用输入参数

操作符：?

引用外部输入参数arguments的值，不在arguments声明则添加一个，运行时被替换。

```
例：
["?update_time__gte"] /* 引用update_time__gte */
["?status|int"] /* 引用status，类型时int型 */
```