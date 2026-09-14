# Caffeine
  - [底层实现](#底层实现)
  - [Caffeine应用步骤](#caffeine应用步骤)
  - [应用场景](#应用场景)
  - [优点](#优点)
  - [缺点](#缺点)
  - [Caffeine VS Guava Cache](#caffeine-vs-guava-cache)
## 底层实现
* W-TinyLFU淘汰算法
    * TinyLFU
        * 使用Count-Min Sketch（CMS）统计访问频率，相比传统LFU，空间占用极小（仅需要几KB）
        * 适用于高并发、低内存场景，避免频繁更新频率计数器带来的性能开销
    * Window TinyLFU
        * 结合SLRU（Segmented LRU）缓存最近访问的数据，避免缓存污染（如突发流量导致高频访问冷数据）
        * Window Cache：存储最近访问的数据（默认1%容量），防止突发流量污染缓存
        * Main Cache：使用TinyLFU进行频率统计，LRU进行淘汰
* 分段锁（Striped Locking）
    * Caffeine使用**分段锁**代替全局锁，提高并发性能：
        * 将缓存分成多个段（Segment），每个段独立加锁
        * 不同线程访问不同段时不会阻塞，减少锁竞争
* 异步加载（Async Loading）- 避免阻塞调用线程
```java
LoadingCache<Key, Value> cache = Caffeine.newBuilder()
    .build(key -> loadDataFromDB(key)); // 同步加载
AsyncLoadingCache<Key, Value> asyncCache = Caffeine.newBuilder()
    .buildAsync(key -> CompletableFuture.supplyAsync(() -> loadDataFromDB(key))); // 异步加载
```
* 写回策略（Write-Behind）
    * 支持异步刷新（Write-Behind），减少磁盘/数据库I/O压力
```java
Cache<Key, Value> cache = Caffeine.newBuilder()
    .writer(new CacheWriter<Key, Value>() {
        @Override
        public void write(Key key, Value value) {
            // 异步写入数据库
        }
    })
    .build();
```

## Caffeine应用步骤
1、创建缓存
（1）无过期策略的缓存
```java
Cache<String, String> cache = Caffeine.newBuilder()
    .maximumSize(1000) // 最大缓存数量
    .build();
```
（2）带过期时间的缓存
```java
Cache<String, String> cache = Caffeine.newBuilder()
    .expireAfterWrite(10, TimeUnit.MINUTES) // 写入后 10 分钟过期
    .expireAfterAccess(5, TimeUnit.MINUTES) // 访问后 5 分钟过期（优先级高于 write）
    .build();
```
（3）基于权重的缓存（适用于大对象）
```java
Cache<String, LargeObject> cache = Caffeine.newBuilder()
    .maximumWeight(10_000) // 最大权重
    .weigher((key, value) -> value.getSize()) // 计算权重
    .build();
```
（4）异步加载缓存
```java
AsyncLoadingCache<String, String> asyncCache = Caffeine.newBuilder()
    .buildAsync(key -> loadDataFromDB(key)); // 异步加载数据
```
2、使用缓存
```java
// 同步缓存
cache.put("key", "value");
String value = cache.getIfPresent("key"); // 获取缓存

// 异步缓存
asyncCache.get("key").thenAccept(value -> {
    System.out.println("Loaded: " + value);
});
```
## 应用场景
* Caffeine 适用于​​：
    * 高并发、低延迟的本地缓存场景（如电商、社交网络）。
    * 需要 ​​高性能、低 GC 影响​​ 的应用。
* ​​不适用场景​​：
    * 分布式缓存（需结合 Redis/Memcached）。
    * 需要持久化的缓存（需额外实现）。

## 优点
1、​​高性能​​：
* 基于 W-TinyLFU 算法，减少内存占用，提高命中率。
* 分段锁设计，支持高并发访问。
2、​​灵活的过期策略​​：
* 支持 expireAfterWrite（写入过期）、expireAfterAccess（访问过期）。
* 支持基于权重的缓存（适用于大对象）。
3、​​异步加载​​：
* 支持 AsyncLoadingCache，避免阻塞调用线程。
4、​​监控友好​​：
* 提供 CacheStats 统计缓存命中率、未命中率等指标。
5、​​低 GC 影响​​：
* 使用 ConcurrentHashMap + 分段锁，减少 Full GC 风险。

## 缺点
1、仅适用于单机
Caffeine 是本地缓存，不支持分布式共享（如 Redis 集群）。
分布式场景需结合 Redis 或 Memcached。
2、无持久化
缓存数据仅在内存中，进程重启后丢失（如需持久化需额外实现）。

## Caffeine VS Guava Cache
|  **对比项**  |         **Caffeine**         |  **Guava Cache**   |
| :----------: | :--------------------------: | :----------------: |
| **淘汰算法** |     W-TinyLFU（更高效）      |   LRU（较简单）    |
| **并发控制** |     分段锁（更高吞吐量）     | 全局锁（性能较低） |
| **过期策略** | 更灵活（支持权重、异步刷新） |       较简单       |
| **异步加载** |             支持             |       不支持       |
|   **监控**   |      提供 `CacheStats`       |         无         |
| **适用场景** |      高并发、高性能缓存      |    简单缓存需求    |
