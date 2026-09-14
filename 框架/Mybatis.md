# Mybatis
  - [Mybatis是如何进行分页的,分页插件的原理是什么：](#mybatis是如何进行分页的分页插件的原理是什么：)
  - [Mybatis 如何将mapper和xml联系起来：](#mybatis如何将-mapper和xml联系起来：)
  - [Mybatis 如何执行批量插入](#mybatis如何执行批量插入)
  - [Mybatis主要原理](#mybatis主要原理)
  - [Mybatis 获取自增主键ID的几种方式：](#mybatis获取自增主键-id的几种方式：)
  - [Mybatis实现多表查询（一对多 、多对一）](#mybatis实现多表查询（一对多、多对一）)
  - [Mybatis是否支持延迟加载，如果支持原理是什么](#mybatis是否支持延迟加载，如果支持原理是什么)
  - [Mybatis缓存机制](#mybatis缓存机制)

## Mybatis是如何进行分页的,分页插件的原理是什么：
Mybatis提供两种分页方式，逻辑分页和物理分页，物理分页更加高效
* 物理分页：通过SQL语句中添加LIMIT和OFFSET字句来实现
* 分页原理：基于Mybatis的拦截器机制，在配置文件中配置分页插件，设置相关参数；执行查询时拦截SQL语句；分页插件会分析拦截到的SQL语句，并根据分页参数修改语句在末尾添加LIMIT和OFFSET语句实现分页功能，最后将修改的SQL语句返回给Mybatis并执行返回结果，最终将结果封装成list或者其他形式返回给调用者。

## Mybatis 如何将mapper和xml联系起来：
将Mapper和XML联系起来主要是依赖于JDK动态代理和Mybatis内部机制实现的
主要过程
1. Mapper接口定义，这个接口包含了数据库的相关操作方法
2. XML映射文件，编写一个与Mapper接口对应的映射文件，这个文件具体包含了SQL语句和参数类型，结果映射信息
3. 当Mybatis启动或者接口被加载时，Mybatis会使用JDK动态代理技术为Mapper接口生成一个代理通常是MapperProxy，这个代理实现了接口，并拦截接口中所有的方法调用
4. 调用与SQL执行，程序调用Mapper接口的方法时，实际是在调用代理类的方法，代理类方法会拦截方法的调用，在根据方法信息与参数在XML中找到相应的SQL语句，然后Mybatis会创建一个SqlSession对象，将方法名和参数传递给SqlSession对象，SqlSession会根据XML映射文件中的配置解析SQL语句，并执行。
5. 结果处理。执行完之后,SqlSession对象会将查询结果返回给Mapper代理类，代理类会将结果转化成java对象，返回给应用程序。

## Mybatis 如何执行批量插入
在Mybatis只从批量插入主要涉及到SqlSession的使用，首先配置Mybatis确保配置文件和xml文件配置正确，从Mybatis配置文件中获取SqlSessionFactory,在使用SqlSessionFactory打开新的SqlSession，使用Mapper接口执行多次插入或者手动构建批量插入的SQL语句，使用SqlSession的insert（）方法执行。插入后提交事务，最后关闭SqlSession。

## Mybatis主要原理
主要通过SqlSessionFactoryBuilder从Mybatis XML配置文件中构建SqlSessionFactory（SqlSessionFactory是线程安全的）
然后SqlSessionFactory实例通过openSession开启SqlSession
通过SqlSession实例的getMapper方法获得指定Mapper对象并运行Mapper映射的SQL语句，完成对数据库的事务提交之后关闭SqlSession。

## Mybatis 获取自增主键ID的几种方式：
1.使用GeneratedKeys标签
2.通过sequence序号，在建表语句中声明sequence到id属性上
3.使用selectKey标签

## Mybatis实现多表查询（一对多 、多对一）
方式1：在sqlMapper配置文件中进行，一对一：在resultMap中association，一对多：在resultMap中collection
方式2：通过注解的方式；一对一：在@Results中@Result下@One
						一对多：在@Results中@Result下@Many
						
## Mybatis是否支持延迟加载，如果支持原理是什么
Mybatis是支持延迟加载的，延迟加载是一种加载数据的策略，只在真正需要数据时才进行加载，有助于提高系统的性能减少资源的消耗
* 原理： 在Mybatis配置文件中或者映射文件中需要将lazyLoadingEnable = true来打开延迟加载功能，当主对象被查询时候，Mybatis会生成一个代理对象，这个代理对象包含了对关联对象的引用，不会立即加载关联对象的数据，当程序需要访问代理对象中的关联属性时，延迟加载就会被触发，此时Mybatis创建一个新的SQL语句来查询相关对象的数据。查询完毕之后Mybatis会将查询到的数据填充到代理对象中使得关联属性可用

* 采用全局延迟加载策略：配置文件中开启全局延迟加载对于所有关联的关系都会按照配置进行延迟加载
* 按需延迟加载：在映射文件中使用FetchType属性按需延迟加载

## Mybatis缓存机制
Mybatis有两级缓存，
* 一级：SqlSession级别 默认开启，无法关闭
* 二级：Mapper级别，默认关闭，需要手动开启

一级缓存 作用域是Session当Sessionflush或者close之后Session中的所有cache将会清空。
二级缓存：作用域为Mapper 使用二级缓存属性需要实现Seralizable序列化接口
对于缓存数据更新机制，当一个作用域（一级缓存Sessiopn/二级缓存Namespaces）进行增加 更新 删除 操作后默认该作用域下所有select中的缓存会被clear，需要在setting全局配置中开启二级缓存，开启二级缓存之后查询顺序为  二级缓存 ->一级缓存->数据库。