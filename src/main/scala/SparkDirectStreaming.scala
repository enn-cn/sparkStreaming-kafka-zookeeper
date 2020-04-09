import cn.fengsong97.tool.{HttpTool, KafkaOffsetManager, ZKPool}
import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.api.OffsetRequest
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

/**
  * Created by fengsong97 on 2020年03月09日22:35:47.
  */
object SparkDirectStreaming {
  val log = org.apache.log4j.LogManager.getLogger("SparkDirectStreaming")

  var appName="Direct Kafka Offset to Zookeeper"
  var logLevel="WARN"

  val brokers="10.38.64.58:9092"; //多个的话 逗号 分隔
  val zkClientUrl="host238.slave.dev.cluster.enn.cn:2181";
  val topicStr="mysql-binlog"; //多个的话 逗号 分隔
  var sparkIntervalSecond=10; //spark 读取 kafka topic 的间隔 秒
  val consumer_group_id="topic001-consumer-group-01"; //消费组 id
  var zkOffsetPath="/kafka/consumers/"+ consumer_group_id + "/offsets";//zk的路径
  var httpGetUrl="https://route.showapi.com/138-46?showapi_appid=171305&prov=%E5%8C%97%E4%BA%AC&showapi_sign=09b0d21daf0d4328b1579a5c7a4a4394"
  var httpPostUrl="https://api.apiopen.top/getSongPoetry?page=1&count=20"

  val isLocal=true//是否使用local模式
  val firstReadLastest=false  //第一次启动,从最新的开始消费, 确保第一次启动时间内,让每个topic的每个分区都存上数,来保存偏移量

  var kafkaParams=Map[String,String](
    "bootstrap.servers"-> brokers,
    "group.id" -> consumer_group_id
  )//创建一个kafkaParams


  def main(args: Array[String]): Unit = {
    //创建StreamingContext
    val ssc=createStreamingContext()
    //开始执行
    ssc.start()
    //等待任务终止
    ssc.awaitTermination()

  }

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

    val ssc=new StreamingContext(sparkConf,Seconds(sparkIntervalSecond))//创建StreamingContext,每隔多少秒一个批次
    ssc.sparkContext.setLogLevel(logLevel)

    val rdds:InputDStream[(String,String)]=createKafkaStream(ssc)
    //处理数据
    processData(rdds)
    ssc//返回StreamContext
  }

  /****
    *
    * @param ssc  StreamingContext
    * @return   InputDStream[(String, String)] 返回输入流
    */
  def createKafkaStream(ssc: StreamingContext): InputDStream[(String, String)]={
    val topicsSet=topicStr.split(",").toSet//topic名字
    val kafkaStream = firstReadLastest match {
      case true =>
        kafkaParams += ("auto.offset.reset"-> OffsetRequest.LargestTimeString)//从最新的开始消费
        //如果firstReadLastest，就说明是系统第一次启动 达到保存 初次偏移量的目的
        log.warn("系统第一次启动，从最新的offset开始消费")
        //使用最新的偏移量创建DirectStream
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      case false =>
        var partitionsForTopics=KafkaOffsetManager.getPartitionsByConsumer(brokers,consumer_group_id,topicsSet)
        val zkClient=ZKPool.getZKClient(zkClientUrl, 30000, 50000)
        var zkOffsetData=KafkaOffsetManager.readOffsets(zkClient,zkOffsetPath,partitionsForTopics)

        log.warn("从zk中读取到偏移量，从上次的偏移量开始消费数据......")
        println(zkOffsetData)
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        //使用上次停止时候的偏移量创建DirectStream
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, zkOffsetData, messageHandler)
    }
    kafkaStream//返回创建的kafkaStream
  }

  /**
   * 开始处理数据
   * @param rdds
   */
  def processData(rdds:InputDStream[(String,String)])= {
    //开始处理数据
    rdds.foreachRDD(rdd => {
      if (!rdd.isEmpty()) { //只处理有数据的rdd，没有数据的直接跳过
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //迭代分区，里面的代码是运行在executor上面
        rdd.foreachPartition(partition => {
          //如果没有使用广播变量，连接资源就在这个地方初始化
          //比如数据库连接，hbase，elasticsearch，solr，等等
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          val topic = o.topic
          var partitionId = o.partition
          var fos = o.fromOffset
          var uos = o.untilOffset
          if (partition.isEmpty) {
          } else {
            println(s"读取 topic: ${topic}, partitionId: ${partitionId}, 起始offset: ${fos}, 终止offset: ${uos}")
            //遍历这个分区里面的消息
            val list = partition.toList
            for (i <- 0 to (uos - fos - 1).toInt) {
              println("数据:" + (fos + i) + ": " + list(i)._2)
              //获取最小单元并验证数据
              var jsonStr = checkData(list(i)._2)
              //发送数据 并提交偏移量
              sendHttp(fos + i,jsonStr)
            }
          }
        })
      }
    })


  }

  def checkData(jsonStr:String): String ={
    //格式验证
    jsonStr
  }

  def sendHttp(index:Long,jsonStr:String): Unit ={
    val reponse = HttpTool.getJson(httpGetUrl, jsonStr);
    val code = reponse.code();
    val body = reponse.body().string();
    if (code == 200) {
      val obj: JSONObject = JSON.parseObject(body); //将json字符串转换为json对象
      println("接口返回数据_" +(index)+ ": " + obj.get("showapi_res_body"))
      //提交偏移量
      // KafkaOffsetManager.saveOffsetPart(zkClientUrl,30000, 20000, zkOffsetPath,topic, partitionId.toString, (fos+i+1).toString )
    } else {
      println("返回错误数据: " + body)
    }
  }
}


