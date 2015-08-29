#horae

![img](http://7xkaaz.com1.z0.glb.clouddn.com/async-invoke-engine-based-on-redis-and-disque_horae.jpg)

***horae*** : 时序女神，希腊神话中司掌季节时间和人间秩序的三女神，又译“荷莱”。

horae是一个基于`redis`和[`disque`](https://github.com/antirez/disque)实现的`轻量级`、`高性能`的异步任务执行器，它的核心是`disque`提供的任务队列，而队列有`先进先出`的时序关系，顾得名：`horae`。

<!-- more -->

horae的关注点不是队列服务的实现本身（已经有不少队列服务的实现了），而是希望借助于`redis`与`disque`提供的纯内存的高性能的队列机制，实现一个异步任务执行器。它可以自由配置任务来自哪种队列服务，它不关注任务执行的最终状态(它写向哪里)或与哪个系统交互，它给你提供一个执行器以及简单地编写任务执行逻辑的方式。

取决于需求，这个执行器在要求不高的时候，只需要一个单节点的redis服务器，即可运转。

如果你愿意牺牲一点性能，来换取更高的队列可靠性保障（这种情况我强烈推荐你使用AMQP协议以及它的开源队列实现：`RabbitMQ`）。如果你想这样，那么这个执行器也是可用的，只是你需要自己去实现跟RabbitMQ交互的细节。你可以用它连接各种其他队列来消费消息并执行任务，它具有充分的扩展性与自由度。但我仍然推荐你使用`disque`。

##适用场景
###抢购/秒杀
抢购业务是典型的短时高并发场景，传统行业里的类似于`学生选课`也可以归结这类场景。

###社交关系处理
纯内存计算/计数器的场景，比如把社交系统里的好友、关系搬到内存中处理。

###耗时的web请求
常见的耗时web请求，比如`生成PDF`、`网页抓取`、`数据备份`、`邮件/短信发送`等。

###分布式系统前端缓冲队列
将它置于应用服务器之后，核心服务之前，作为请求的缓冲队列使用。

>概括起来就是`服务器峰值扛压`、`异步处理`、`纯内存计算`，当然你把它用成普通队列也是可以的。

##高性能
目前支持`disque`跟`redis`这两种队列服务（主推`disque`，`redis`的队列暂时以`list`数据接口的`lpush`&`brpop`实现，但它不是高可靠的，并且没有ack机制）。这两种纯内存的队列首先保证了消费任务的性能。具体任务的执行性能，取决于使用场景，这里分析两种场景：

###纯内存&单线程&无锁
如果任务处理器消费的消息是完全存储于内存中的，那么需要尽量将同构的各任务访问的数据进行隔离（隔离的手段是对key划分命名空间），如果实在没办法隔离，可以使用单队列单线程无锁的处理方式。

###通用&多线程&多队列
如果是通用的应用场景，比如访问数据库，因为数据库有成熟的数据一致性保证。所以，你可以将任务划分到多个不同的队列，并利用多个线程来并发执行以加快任务的处理效率。

当然最推荐的使用方式是：用`redis`作为配置、协调、管控中心，用`disque`做队列服务，任务需要访问的数据尽可能存储于`redis`中。

##高可用

###一主多从
执行器在运行时实行的是：Master单节点运行，多个Slave做Standby的机制来保证服务的可用性。事实上，从Master下线到其中一个Slave成功竞选为Master需要数个心跳周期的时间。因为执行器作为队列的消费者跟队列是完全解耦的，所以短暂的暂停消费对整个系统的可用性不会产生太大影响。

###心跳机制
Master跟Slave之间通过`redis-Pubsub`来维持心跳。目前的设计是Master单向`publish`心跳，Slave`subscribe`Master的心跳。这么设计的原因是简单，并且考虑到每个Slave都是无状态的执行器，并不会涉及到状态的维护与同步问题，所以Master不需要关心Slave的存活。

###竞争Master
一旦Master下线（比如因为故障宕机），需要快速得从多个Slave中选举出一个新的Master，选举的算法非常多，并且非常复杂。

通常选举Master的方式会由一个独立的承担`Manager`角色的节点来完成，如果不存在这样一个节点那么通常会基于分布式选举算法来实现（`Zookeeper`比较擅长这个）。这里简单得采用类似于竞争分布式锁的实现方式来抢占Master。

如何判断Master是否下线？这是一个非常关键的问题，因为如果产生误判，将会给整体系统服务造成一段空档期，这是一个不小的时间开销。采用的判断方式是**双重检测**：

* Slave订阅Master的heartbeat channel，判断心跳是否超时
* Slave去Master的数据结构中去获取Master自己刷新的心跳时间戳，并跟当前时间对比，判断是否超时

具体的实现方式：每个服务都会有一个heartbeat线程，Master的heartbeat线程做两件事情：

* refresh自己的心跳时间戳
* publish自己的心跳到`heartbeat` channel

Slave的heartbeat线程做上面的**双重检测**，Slave会等待几个心跳周期，如果在这段时间内，两种检测都认为Master失去心跳，则判断Master下线。

Master下线后，就涉及到多个Slave竞争Master的问题，这里我们在竞争锁的时候没有采用阻塞等待的方式，而是采用了一种危险性相对小的方式：`tryLock`:

```
    private boolean tryLockMaster() {
        long currentTime = RedisConfigUtil.redisTime(configContext);
        String val = String.valueOf(currentTime + Constants.DEFAULT_MASTER_LOCK_TIMEOUT + 1);
        Jedis jedis = null;
        try {
            jedis = RedisConfigUtil.getJedis(configContext).get();
            boolean locked = jedis.setnx(Constants.KEY_LOCK_FOR_MASTER, val) == 1;
            if (locked) {
                jedis.expire(Constants.KEY_LOCK_FOR_MASTER,
                             Constants.DEFAULT_MASTER_LOCK_TIMEOUT);

                return true;
            }
        } finally {
            if (jedis != null) RedisConfigUtil.returnResource(jedis);
        }

        return false;
    }
```

只有判断Master下线之后，才会调用`tryLockMaster`，它仅仅是尝试获得锁，如果获取成功，将给锁设置一个很短的过期时间，这里跟跟心跳过期时间相同。如果获取失败将继续检测心跳。获取锁的Slave会立即变为Master并迅速刷新自己的心跳，这样，其他Slave检测Master下线就会失败，将不会再去调用`tryLockMaster`。避免了通常情况下，一直阻塞、竞争锁这一条路。

##扩展性
###扩展功能
得益于Redis的`PubSub`，我们可以实现很多类似于`指令下发->执行`的feature，比如实时获取任务的执行进度、让各服务器汇报自己的状态等。因为时间关系，目前这块只是留了一个扩展口：

* 上行频道：执行器有一个`upstream` channel，用于上传各节点的本机信息。
* 下行频道：系统有一个`downstream` channel，用于被动接受来自上游的信息/指令。

这里上下游的语义是：所有服务节点均为下游，`redis`配置中心应该算是中心节点，在上游你可以定制一个管控台，用于管理`redis`配置中心并向下游的服务节点下发指令。

###扩展队列服务
如果你想扩展它，希望它支持另一种队列服务（为了方便表述，这里假设你想支持RabbitMQ）。那么你需要做以下几步：

* 在package：`com.github.horae.exchanger`包下新建类：`RabbitConnManager`用于管理client 到 RabbitMQ的连接
* 同样在package：`com.github.horae.exchanger`包下新建类：`RabbitExchanger`用于实现消息的出队与入队逻辑，该类需实现`TaskExchanger`
* 在`TaskExchangerManager`的`createTaskExchanger`方法内加入新的分支判断。
* 在`partition.properties`下可以配置新的partition，在matrix中指定RabbitMQ

需要**注意**的是：`TaskExchanger`的`dequeue`接口方法，默认的行为是`block`形式的。如果你扩展的队列不支持block形式的消费，那可能需要你自己实现，实现的方式可以借助于`java.util.concurrent.BlockingQueue`。

##多种可靠性级别
队列的可靠性牵扯到整个分布式系统的可靠性，这是一个无法回避的问题。如果你说用`redis`实现的队列，是否能做到既保持`高性能`又能兼具`高可靠`，答案是`不能`。或者说它不是一个专业的队列服务（不然redis的作者也没有必要再另起`disque`项目了）。如果从可靠性的角度而言，我给几个主流的队列服务器（或者可以提供队列服务）的排名是：`RabbitMQ` > `Kafka` > `Disque` > `Redis`。虽然这个执行器内置支持了`disque`和`redis`作为队列的实现，但它跟你选择的队列服务没有非常紧的耦合关系，你可以选择其他队列服务，通常你只需要实现这么几个功能`入队消息`、`出队消息`、`ack消息`、`管理连接`。

##分区
对我而言`分区`的概念来自于Kafka，但这里的分区跟Kafka性质不太一样。首先我们来看为什么有这样的需求？

作为一个无状态的服务，它可以长时间运行（某种程度上，这有点像Storm）而不必下线。为了充分榨取CPU的价值。我们可能希望在一次服务的生命周期内让它运行多个异构服务（所谓异构任务，就是不同性质的任务）。因此我们有必要将多个异构任务区分开来，而这个手段就是`分区`。说它不同于kafka的原因是：它更多是一种逻辑上的划分，而不是kafka物理上按分区存储消息。我们来看一个分区隔离了哪些东西：

```
partition.root=p0,p1
p0.matrix=redis
p0.host=127.0.0.1
p0.port=6379
p0.class=com.github.horae.task.RedisTask
p1.matrix=disque
p1.host=127.0.0.1
p1.port=7711
p1.class=com.github.horae.task.DisqueTask
```

* matrix : 哪种队列实现服务，目前支持`disque`/`redis`
* host : 队列服务器的host
* port : 队列服务器的port
* class : 处理队列任务的实现类的完全限定名

从上面的隔离方式来看，这里的分区也能做到对任务队列的物理隔离。上面配置了两个分区，两个分区分别对应了两种队列服务。分区跟队列服务的对应关系没有限制，甚至多个分区对应一个队列服务器也可行，因为还有一个分区到队列名称的映射关系：

如下图：

![img](http://7xkaaz.com1.z0.glb.clouddn.com/async-invoke-engine-based-on-redis-and-disque_partition.png)


综述：分区隔离了异构任务的队列，而队列存储于何种队列服务、存储于何处、以及任务的处理逻辑完全取决于配置。

上面的解析明确了分区跟任务处理类的对应关系。为了便于管理，一个分区也有其独立的线程池来将异构任务的线程隔离开来。

##编写任务处理器
在你编写一个任务处理器之前，你应该意识到你编写的任务处理器充当的是`队列的消费者`。接下来你需要了解的是，你编写的任务处理器将在一个线程池中运行，而线程池的管理，需要你关心，但你需要知道：`一个任务队列将会对应一个线程`。你需要知道的就是这么多，下面来编写一个任务处理器：

* 首先你需要创建一个新的maven工程
* 在horae发布包的库目录下(`./horae/libs`)找到以`horae`开头的jar文件，加入到你的maven依赖中，只是一个本地依赖：

```
        <dependency>
            <groupId>com.github.horae</groupId>
            <artifactId>horae</artifactId>
            <version>0.1.0</version>
            <scope>system</scope>
            <systemPath>/usr/local/horae/libs/horae-0.1.0.jar</systemPath>
        </dependency>
```

* 你需要新建一个类，继承`TaskTemplate `，并实现`run`方法，下面是一个模板：

```java
    public void run() {
        try {
            signal.await();
            
            //implement task process business logic
        } catch (InterruptedException e) {

        }
    }
```

* 编写构造方法：

```
    public RedisTestTask(CountDownLatch signal, String queueName, Map<String, Object> configContext,
                         Partition partition, TaskExchanger taskExchanger) {
        super(signal, queueName, configContext, partition, taskExchanger);
    }
```

在run方法的第一句，你需要调用一个`CountDownLatch`实例的`await`方法来将其阻塞住。解释一下，为什么需要这么做？

其实，每个服务在启动的时候，都会立即读取redis内配置的队列，并初始化线程池，进入执行就绪状态。这一步，所有的服务，无论是Master，Slave都是一样的。但区别就区别在这句：

```
signal.await();
```

当启动的是master节点，那么该signal会立即释放信号(通过`signal.countDown()`)，所有任务处理器都立即开始执行。

而启动的是slave节点，则将会一直在上面这句代码这里阻塞，直到master下线，而该节点竞争到master之后，会立即释放解除阻塞信号，后续代码会立即执行。

因此这么做可以使得在master下线之后，所有Slave都以最快的速度进入任务执行状态，虽然对一些Slave节点而言，这有些浪费系统资源。

* 编译工程并打包jar，注意不用包含上面的maven依赖，它已经存在于`horae`可执行文件类库中。

* 将生成的jar放置于`./horae/libs/`下，它将会被自动添加到`classpath`中

* 编辑配置文件`./horae/conf/partition.properties`，新建/修改一个分区的`p{x}.class`，值为你刚刚编写的任务实现类的**完全限定名**。

##安装部署

>以下安装步骤在Mac OS X系统验证通过（Linux系类似，但存在一些不同）。Mac用户需要预装`Homebrew`

* 安装jsvc

```
brew install jsvc
```

* 安装redis

```
brew install redis
```

* 安装disque

>因为disque目前还没有一个稳定的版本，所以暂时被homebrew暂存在[head-only](https://github.com/Homebrew/homebrew-head-only) 仓库中，安装命令略有不同：

```
brew install --HEAD homebrew/head-only/disque
```

* `horae`源码编译、打包

```
mvn assembly:assembly
```

* 拷贝打包文件到目标文件夹，并解压缩

```
cd ${project_baseDir}/target/horae.zip /usr/local
unzip /usr/local/horae.zip
```

* 配置可执行文件，主要是命令与路径

```
sudo vi /usr/local/horae/bin/horae.sh
```

* 配置conf下的配置文件

```
sudo vi /usr/local/horae/conf/${service/redisConf/partition}.properties
```

* 执行命令

```
sudo sh /usr/local/horae/bin/horae.sh ${start/stop/restart}
```

###注意点
* conf下的service.properties中的配置项`master`在所有节点中只能有一个被设置为true。如果它下线，将不能以master的身份再次启动。
* 因为jsvc需要写进程号(pid)，所以尽量以系统管理员身份执行，将horae.sh里的`user`配置为`root`，并以`sudo`执行

##关于disque

目前disque仍处于alpha版本，命令也还在调整中。虽然已被支持，但无论是disque的server以及其java client:`jedisque`都存在bug，因此暂时**不推荐**使用，请至少等到发布stable版本再使用。

自实现的`jedisque`连接池。目前jedisque的客户端还没有提供连接池机制，它跟redis的主流java client：`jedis`出自同一个开发者手笔。考虑到`jedis`内部使用的是`apache commons-pool`实现连接池机制，在实现`jedisque`的时候也使用的是同样的方案，等`jedisque`官方提供连接池之后，会采用官方连接池。

`disque`的开发过程中，对命令和命令参数可能会进行调整，`horae`也会对此进行跟进。虽然，`disque`的stable版本还未发布，但redis作者的水准和口碑有目共睹，所以你有理由相信它能给你带来惊喜。

##licence
Copyright (c) 2015 yanghua. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

```
http://www.apache.org/licenses/LICENSE-2.0
```
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.