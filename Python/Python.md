# Python
  - [异常](#异常)
  - [Lambda匿名函数](#lambda匿名函数)
  - [Python基本数据类型？](#python基本数据类型？)
  - [Python复制列表的方法？](#python复制列表的方法？)
  - [is 和 == 的区别？](#is和的区别？)
  - [列表和元组的区别？](#列表和元组的区别？)

## 异常
使用`try`...`except`语句来捕获异常打印日志，处理错误并继续执行脚本，这样，遇到错误时可以捕获异常，打印错误信息，并让脚本继续执行。

```python
def test_case_1():
    try:
        # 执行可能会出错的代码
        data = get_data()  # 假设这个函数可能获取不到数据
        print(f"Data: {data}")
    except Exception as e:
        # 捕获异常并输出错误信息
        print(f"Error occurred in test_case_1: {e}")
        # 你可以选择在这里进行一些处理，比如记录日志、设置默认值等
    # 无论是否有异常，脚本都会继续执行
    print("Test case 1 finished, continue with the next test...")

def test_case_2():
    try:
        # 执行另一个可能出错的操作
        result = perform_calculation()  # 假设这个函数也可能出错
        print(f"Calculation result: {result}")
    except Exception as e:
        # 捕获异常并输出错误信息
        print(f"Error occurred in test_case_2: {e}")
    print("Test case 2 finished, continue with the next test...")

# 主函数
def main():
    test_case_1()
    test_case_2()
    print("All tests completed.")

if __name__ == "__main__":
    main()
```

## Lambda匿名函数

使用场景：只需要用一次的简洁操作
语法：lambda 参数 : 表达式
lambda 函数通常与内置函数如 map()、filter() 、sorted（）和 reduce() 一起使用，以便在集合上执行操作。例如：

```python
add = lambda x, y: x + y
print(add(3, 5))  # 输出 8

# 筛选出偶数
numbers = [1, 2, 3, 4, 5, 6, 7, 8] even_numbers = list(filter(lambda x: x % 2 == 0, numbers))

points = [(2, 3), (1, 5), (3, 1)]
points_sorted = sorted(points, key=lambda x: x[1])
print(points_sorted)  # 输出 [(3, 1), (2, 3), (1, 5)]

nums = [1, 2, 3, 4]
squares = map(lambda x: x ** 2, nums)
print(list(squares))  # 输出 [1, 4, 9, 16]
```

## Python基本数据类型？

* 数字类型：`int`, `float`

* 序列类型：`string`, `list列表`, `tuple元组`
> String：通过索引访问字符，切片来获取子字符串
> list：列表是有序的，允许包含重复元素。支持索引、切片、遍历等操作。
> tuple：元素有序，不允许修改，添加，删除，可以包含重复元素，元组内通常存储，不需要修改的数据

* 映射类型：`dict`
> dict：每个键都是唯一的，键必须是不可变类型（字符串、数字、元组）

* 集合类型：`set集合`
> set：集合无需，不允许重复元素，适合去重操作

* 布尔类型：`bool`

* 二进制类型：`bytes`

* 特殊类型：`None`

## Python复制列表的方法？

- **浅拷贝**：`[:]`, `list()`, `copy()`, `copy.copy()`, `extend()`, 列表推导式, `for` 循环
- **深拷贝**：`copy.deepcopy()`

**浅拷贝**：复制最外层的对象，但内部的对象引用原始数据。

**深拷贝**：递归地复制整个对象，包括嵌套的对象。

## is 和 == 的区别？

**`==`**: 比较的是对象的**值**是否相等。

**`is`**: 比较的是对象的**身份**（即内存地址）是否相同。例如在检查 `None` 时，`is None` 更为推荐。

## 列表和元组的区别？

**列表**：列表使用方括号 **[ ]**是**可变**的，可以修改、添加或删除元素，适用于需要频繁修改的场景。

**元组**：使用小括号 **( )**，是**不可变**的，一旦创建，元素不能修改、添加或删除，适用于数据固定且需要提高性能或作为字典键的场景。

> **列表**：不能作为字典的键，因为列表是可变的，不能被哈希。