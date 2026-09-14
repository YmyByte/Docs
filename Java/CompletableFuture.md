# CompletableFuture
  - [为什么要使用CompletableFuture：](#为什么要使用completablefuture：)
  - [为什么会选择CompletableFuture？](#为什么会选择completablefuture？)
  - [CompletableFuture谈谈理解：](#completablefuture谈谈理解：)
  - [你如何使用Java的CompletableFuture来实现异步编程？](#你如何使用java的completablefuture来实现异步编程？)
  - [CompletableFuture底层原理：](#completablefuture底层原理：)
  - [你能给我一个例子，说明你如何处理CompletableFuture中的错误和异常？](#你能给我一个例子，说明你如何处理completablefuture中的错误和异常？)
  - [为什么还是串行但是性能大大提高了呢？](#为什么还是串行但是性能大大提高了呢？)

## 为什么要使用CompletableFuture：
**1.简化异步编程**：在传统的异步编程中，处理线程间的通信和数据传递通常需要复杂的同步机制，如 wait/notify、CountDownLatch、CyclicBarrier 等。CompletableFuture 提供了一系列的方法，如 thenApply、thenAccept、thenRun 等，使得异步操作的编排变得更加简单直观。
**2.支持非阻塞操作**：CompletableFuture 支持非阻塞的异步编程模式。开发者可以在不阻塞当前线程的情况下，安排一个异步操作，并在操作完成时接收通知。这种方式可以提高应用程序的响应性和吞吐量。
**3.丰富的组合操作**：CompletableFuture 提供了多种方法来组合多个异步操作，例如 thenCombine、thenCompose、allOf、anyOf 等。这些方法可以帮助开发者轻松实现复杂的异步流程控制。
**4.异常处理**：CompletableFuture 允许开发者通过 exceptionally 和 handle 方法来处理异步操作中发生的异常，这样可以避免异常导致的线程中断和应用程序崩溃。
**5.支持取消操作**：CompletableFuture 支持取消异步操作。如果某个异步任务不再需要，可以通过 cancel 方法来取消它，从而节省系统资源。
**6.更好的并发控制**：CompletableFuture 可以与 Java 的其他并发工具（如 ExecutorService）


## 为什么会选择CompletableFuture？
对比同类型的其他异步方案主要包括Future、CompletableFuture、RxJava、Reactor。它们的特性。
RxJava与Reactor显然更加强大，它们提供了更多的函数调用方式，支持更多特性，但同时也带来了更大的学习成本。而我们项目里面最需要的特性就是“异步”、“可组合”，综合考虑后，我们选择了学习成本相对较低的CompletableFuture。
**补充**
**RxJava与Reactor具有的：操作融合**：将数据流中使用的多个操作符以某种方式结合起来，进而降低开销（时间、内存）。
**延迟执行**：操作不会立即执行，当收到明确指示时操作才会触发。例如Reactor只有当有订阅者订阅时，才会触发操作。
**回压**：某些异步阶段的处理速度跟不上，直接失败会导致大量数据的丢失，对业务来说是不能接受的，这时需要反馈上游生产者降低调用量。

## CompletableFuture谈谈理解：
CompletableFuture是JDK1.8里面引入的一个基于事件驱动的异步回调类。
简单来说，就是当使用异步线程去执行一个任务的时候，我们希望在任务结束以后触发一个后续的动作。
而CompletableFuture就可以实现这个功能。举个简单的例子，比如在奥坦途车辆管理系统中，其中一个功能是车辆碰撞响应，逻辑是：车辆硬件设备发出碰撞响应请求到服务端接口，这个时候后端需要去查询移动接口获取当前位置信息，以及获取车主基本信息。而这种设计方式导致这个方法的执行性能比较慢。
所以，这里可以直接使用CompletableFuture，也就是说把查询订单的逻辑放在一个异步线程池里面去处理。
然后基于CompletableFuture的事件回调机制的特性，可以把两个任务组合在一起，当两个任务都执行结束以后触发事件回调。从而极大的提升这个这个业务场景的处理性能。
而且CompletableFuture有5种不同的执行方式，把多个异步任务组成一个具有先后关系的处理链，然后基于事件驱动任务链的执行。①是把两个任务组合一起，当两个任务执行结束后触发事件回调。②把两个任务组合一起两个任务串行执行，第一个任务执行完以后自动触发执行第二个任务。③第一个任务执行结束后触发第二个任务，并且第一个任务的执行结果作为第二个任务的参数，这个方法是纯粹接受上一个任务的结果，不返回新的计算值。④跟上一个一样，但是它有返回值⑤就是第一个任务执行完成后触发执行一个实现了Runnable接口的任务。
最后，我认为，CompletableFuture弥补了原本Future的不足，使得程序可以在非阻塞的状态下完成异步的回调机制。

## 你如何使用Java的CompletableFuture来实现异步编程？
Java的CompletableFuture是Java 8引入的一个类，用于实现异步编程。它提供了一种简洁的方式来编写异步代码，使得代码更加易读和易于理解。
首先创建一个CompletableFuture对象。这个对象代表了一个可能还没有完成的计算任务。
使用CompletableFuture的thenApply、thenAccept、thenRun或者thenCompose方法来定义在计算完成后需要执行的操作。这些方法都会返回一个新的CompletableFuture对象，这样你就可以继续在这个新的CompletableFuture对象上添加更多的操作。
当所有的操作都完成时，你可以使用CompletableFuture的join方法来获取结果。如果计算还没有完成，那么join方法会阻塞，直到计算完成为止。

## CompletableFuture底层原理：
CompletableFuture的底层实现使用了一种基于**回调和事件驱动**的方式来实现异步操作。在CompletableFuture中，每个异步操作都是一个独立的任务，当任务完成时，将会触发相应的回调函数来处理结果或异常。
CompletableFuture基本架构中：
首先CompletableFuture 实现了 CompletionStage 接口，这是它支持连续异步操作的基础。每个 CompletionStage 可以视为异步操作的一个阶段，每个阶段完成后可以触发下一个阶段，形成一个操作链。
对于异步操作与回调：内部，CompletableFuture 维护了一个任务列表，这些任务表示在当前 Future 完成后应当执行的动作。这些动作通常是一些回调函数，它们可以在 CompletableFuture 的结果可用时被自动调用。
异步任务的执行过程中：
在 CompletableFuture 的内部，每个阶段的任务可以指定 Executor 执行。如果没有指定，任务默认在 ForkJoinPool.commonPool() 中执行。这个过程是完全非阻塞的，即调用线程可以立即继续执行其他代码。通过这种方式，CompletableFuture 实现了任务的异步执行，同时减少了对线程的直接管理需求。
由于任务是在 Executor 中异步执行的，CompletableFuture 的主要操作（如 thenApply, thenAccept 等）本身不会阻塞调用者线程，能够进行快速的响应。
CompletableFuture 允许手动或自动完成：
可以通过 complete(T value) 手动设置结果。
可以通过 completeExceptionally(Throwable ex) 手动抛出异常。
当异步任务执行完成后，它会自动触发设置的结果。
一旦 CompletableFuture 完成，如果CompletableFuture还有依赖任务（异步），会将任务加入到CompletableFuture的堆栈保存起来。以供后续完成后执行依赖任务。
CompletableFuture 通过 handle, exceptionally 等方法提供了处理异常的能力
**补充**：
thenApply：用于将一个CompletableFuture对象的结果转换为另外一个CompletableFuture对象的结果；
thenAccept：用于对CompletableFuture的结果进行消费；
thenRun：用于在CompletableFuture完成后执行一个Runnable对象；
thenCombine：用于将两个CompletableFuture对象的结果进行组合；
thenCompose：用于将一个CompletableFuture对象的结果作为下一个CompletableFuture对象的输入。
                        
## 你能给我一个例子，说明你如何处理CompletableFuture中的错误和异常？
在Java的CompletableFuture中，错误和异常的处理主要通过使用exceptionally、handle和orElse方法来实现。
* exceptionally：这个方法会在CompletableFuture完成时执行，无论其正常完成还是异常完成。它接受一个Function类型的参数，这个函数会被应用到CompletableFuture的异常结果上。
* handle：这个方法也接受一个Function类型的参数，但是这个函数会被应用到CompletableFuture的结果或者异常上，而不是异常本身。
* orElse：这个方法会在CompletableFuture正常完成时执行，如果CompletableFuture抛出异常，那么它会返回一个默认值。

## 为什么还是串行但是性能大大提高了呢？
因为不需要同步等待返回结果，而是异步去回调触发。在回调之前可以做其他的事情。如果没有回调机制，我们需要通过阻塞等待，相当于省略了阻塞的动作。