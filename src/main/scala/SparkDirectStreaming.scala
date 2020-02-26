import kafka.api.OffsetRequest
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by QinDongLiang on 2017/11/28.
  */
object SparkDirectStreaming {


  val log = org.apache.log4j.LogManager.getLogger("SparkDirectStreaming")

  var appName="Direct Kafka Offset to Zookeeper"
  var logLevel="WARN"

  val brokers="127.0.0.1:9092"; //多个的话 逗号 分隔
  val zkClientUrl="127.0.0.1:2181";
  val topicStr="topic001,topic002"; //多个的话 逗号 分隔
  var sparkIntervalSecond=10; //spark 读取 kafka topic 的间隔 秒
  val consumer_group_id="topic001-consumer-group-01"; //消费组 id
  var zkOffsetPath="/kafka/consumers/"+ consumer_group_id + "/offsets";//zk的路径

  val isLocal=true//是否使用local模式
  val firstReadLastest=false  //第一次启动,从最新的开始消费, 确保第一次启动时间内,让每个topic的每个分区都存上数,来保存偏移量


  var kafkaParams=Map[String,String](
    "bootstrap.servers"-> brokers,
    "group.id" -> consumer_group_id
  )//创建一个kafkaParams


  /***
    * 创建StreamingContext
    * @return
    */
  def createStreamingContext():StreamingContext={

    val sparkConf=new SparkConf().setAppName(appName)
    if (isLocal)  sparkConf.setMaster("local[1]") //local模式
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")//优雅的关闭
    sparkConf.set("spark.streaming.backpressure.enabled","true")//激活削峰功能
    sparkConf.set("spark.streaming.backpressure.initialRate","5000")//第一次读取的最大数据值
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","2000")//每个进程每秒最多从kafka读取的数据条数

    if (firstReadLastest)   kafkaParams += ("auto.offset.reset"-> OffsetRequest.LargestTimeString)//从最新的开始消费
    //创建zkClient注意最后一个参数最好是ZKStringSerializer类型的，不然写进去zk里面的偏移量是乱码
    val zkClient=ZKPool.getZKClient(zkClientUrl, 30000, 20000)

    val topicsSet=topicStr.split(",").toSet//topic名字

    val ssc=new StreamingContext(sparkConf,Seconds(sparkIntervalSecond))//创建StreamingContext,每隔多少秒一个批次
    ssc.sparkContext.setLogLevel(logLevel)

    val rdds:InputDStream[(String,String)]=createKafkaStream(ssc,kafkaParams,zkClient,zkOffsetPath,topicsSet)

    //开始处理数据
    rdds.foreachRDD( rdd=>{

      if(!rdd.isEmpty()){//只处理有数据的rdd，没有数据的直接跳过

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //迭代分区，里面的代码是运行在executor上面
        rdd.foreachPartition(partition=>{

          //如果没有使用广播变量，连接资源就在这个地方初始化
          //比如数据库连接，hbase，elasticsearch，solr，等等

          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          val topic=o.topic
          var partitionId=o.partition
          var fos =o.fromOffset
          var uos =o.untilOffset

          if(partition.isEmpty){
          }else{
              println(s"读取 topic: ${topic}, partitionId: ${partitionId}, 起始offset: ${fos}, 终止offset: ${uos}")

              //遍历这个分区里面的消息
              val list= partition.toList
              for (i <- 0 to (uos-fos-1).toInt ) {
                println("数据:"+(fos+i)+": "+list(i)._2)

                //提交偏移量
                KafkaOffsetManager.saveOffsetPart(zkClientUrl,30000, 20000, zkOffsetPath,topic, partitionId.toString, (fos+i+1).toString )
              }
          }
          

        })

        //更新每个批次的偏移量到zk中，注意这段代码是在driver上执行的
//        KafkaOffsetManager.saveOffsets(zkClient,zkOffsetPath,rdd)
      }


    })


    ssc//返回StreamContext


  }






  def main(args: Array[String]): Unit = {

    //创建StreamingContext
    val ssc=createStreamingContext()
    //开始执行
    ssc.start()
    //等待任务终止
    ssc.awaitTermination()

  }

  /****
    *
    * @param ssc  StreamingContext
    * @param kafkaParams  配置kafka的参数
    * @param zkClient  zk连接的client
    * @param zkOffsetPath zk里面偏移量的路径
    * @param topicsSet     需要处理的topic
    * @return   InputDStream[(String, String)] 返回输入流
    */
  def createKafkaStream(ssc: StreamingContext,
                        kafkaParams: Map[String, String],
                        zkClient: ZkClient,
                        zkOffsetPath: String,
                        topicsSet: Set[String]): InputDStream[(String, String)]={
    //目前仅支持一个topic的偏移量处理，读取zk里面偏移量字符串
    var zkOffsetData=KafkaOffsetManager.readOffsets(zkClient,zkOffsetPath,topicsSet)

    val kafkaStream = firstReadLastest match {
      case true =>
        //如果firstReadLastest，就说明是系统第一次启动 达到保存 初次偏移量的目的
        log.warn("系统第一次启动，从最新的offset开始消费")
        //使用最新的偏移量创建DirectStream
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

      case false =>
        log.warn("从zk中读取到偏移量，从上次的偏移量开始消费数据......")
        println(zkOffsetData)

        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        //使用上次停止时候的偏移量创建DirectStream
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, zkOffsetData, messageHandler)
    }
    kafkaStream//返回创建的kafkaStream
  }





}


