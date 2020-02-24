# streaming-offset-to-zk
## 项目背景
公司有一个比较核心的实时业务用的是spark streaming2.1.0+kafka0.9.0.0的流式技术来开发的，存储用的hbase+elasticsearch+redis，这中间趟过很多坑，解决了一些bug和问题，在这里我把它做成了一个骨架项目并开源出来，希望后来的朋友可以借阅和参考，尽量少走些弯路，当然如果中间遇到或者发现什么问题都可以给我提issue。

下面是使用过程中记录的一些心得和博客，感兴趣的朋友可以了解下：

（1）[spark streaming自带的checkpoint容错使用](http://qindongliang.iteye.com/blog/2350846) 

（2）[spark streaming自带的checkpoint的弊端](http://qindongliang.iteye.com/blog/2356634) 

（3）[如何管理spark streaming消费Kafka的偏移量（一）](http://qindongliang.iteye.com/blog/2399736) 

（4）[如何管理spark streaming消费Kafka的偏移量（二）](http://qindongliang.iteye.com/blog/2400003) 

（5）[如何管理spark streaming消费Kafka的偏移量（三）](http://qindongliang.iteye.com/blog/2401194) 

（6）[spark streaming程序如何优雅的停止服务（一）](http://qindongliang.iteye.com/blog/2364713) 

（7）[spark streaming程序如何优雅的停止服务（二）](http://qindongliang.iteye.com/blog/2401501) 

（8）[spark streaming程序如何优雅的停止服务（三）](http://qindongliang.iteye.com/blog/2404100) 





## 项目简介
该项目提供了一个在使用spark streaming2.1+kafka0.9.0.0的版本集成时，手动存储偏移量到zookeeper中，因为自带的checkpoint弊端太多，不利于
项目升级发布，并修复了一些遇到的bug，例子中的代码已经在我们生产环境运行，所以大家可以参考一下。


## 主要功能

（1）提供了快速使用 spark streaming + kafka 开发流式程序的骨架，示例中的代码大部分都加上了详细的注释

（2）提供了手动管理kafka的offset存储到zookeeper的方法，并解决了一些bug，如kafka扩容分区，重启实时流不识别新增分区的问题。

（3）提供了比较简单和优雅的关闭spark streaming流式程序功能



