import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges

/**
  *
  * 负责kafka偏移量的读取和保存
  *
  * Created by QinDongLiang on 2017/11/28.
  */
object KafkaOffsetManager {


  lazy val log = org.apache.log4j.LogManager.getLogger("KafkaOffsetManage")

//   def readOffsets(zkClient: ZkClient, zkOffsetPath: String, topic: String): Option[Map[TopicAndPartition, Long]] = {
//
//
//     //（偏移量字符串,zk元数据)
//    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkOffsetPath)//从zk上读取偏移量
//    offsetsRangesStrOpt match {
//      case Some(offsetsRangesStr) =>
//        //这个topic在zk里面最新的分区数量
//        val  lastest_partitions= ZkUtils.getPartitionsForTopics(zkClient,Seq(topic)).get(topic).get
//        var offsets = offsetsRangesStr.split(",")//按逗号split成数组
//          .map(s => s.split(":"))//按冒号拆分每个分区和偏移量
//          .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }//加工成最终的格式
//          .toMap//返回一个Map
//
//        //说明有分区扩展了
//        if(offsets.size<lastest_partitions.size){
//          //得到旧的所有分区序号
//          val old_partitions=offsets.keys.map(p=>p.partition).toArray
//          //通过做差集得出来多的分区数量数组
//          val add_partitions=lastest_partitions.diff(old_partitions)
//          if(add_partitions.size>0){
//            log.warn("发现kafka新增分区："+add_partitions.mkString(","))
//            add_partitions.foreach(partitionId=>{
//              offsets += (TopicAndPartition(topic,partitionId)->0)
//              log.warn("新增分区id："+partitionId+"添加完毕....")
//            })
//
//          }
//
//        }else{
//          log.warn("没有发现新增的kafka分区："+lastest_partitions.mkString(","))
//        }
//
//
//        Some(offsets)//将Map返回
//      case None =>
//        None//如果是null，就返回None
//    }
//  }

  def readOffsets2(zkClient: ZkClient, zkOffsetPath: String, topicSet: Set[String]): Map[TopicAndPartition, Long] = {
    var topicMap= ZkUtils.getPartitionsForTopics(zkClient,topicSet.toList)
    val topics = topicMap.keys;


    var offsets =topics.flatMap(key=>{
      topicMap.get(key).get.toList.map(part =>{
        var (f, _) =ZkUtils.readDataMaybeNull(zkClient, zkOffsetPath + "/"+key+"/" + part.intValue());
        f match {
          case None => {
            log.warn(s"发现kafka新增分区：${key} 分区 ${part.intValue()}")
            var frn =0
            (TopicAndPartition(key, part.intValue()) -> frn.toLong)
          }
          case Some(num) =>
            (TopicAndPartition(key, part.intValue()) -> num.toLong)
        }
      })
    })

    offsets.toMap
  }

  /****
    * 保存每个批次的rdd的offset到zk中
    * @param zkClient zk连接的client
    * @param zkOffsetPath   偏移量路径
    * @param rdd     每个批次的rdd
    */
  def saveOffsets(zkClient: ZkClient, zkOffsetPath: String, rdd: RDD[_]): Unit = {
    //转换rdd为Array[OffsetRange]
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //转换每个OffsetRange为存储到zk时的字符串格式 :  分区序号1:偏移量1,分区序号2:偏移量2,......
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.untilOffset}").mkString(",")
    log.warn(" 保存的偏移量：  "+offsetsRangesStr)
    //将最终的字符串结果保存到zk里面
//        ZkUtils.updatePersistentPath(zkClient, zkOffsetPath, offsetsRangesStr)
  }

  def saveOffsetPart(zkClientUrl: String,sessionTimeout: Int, connectionTimeout: Int, zkOffsetPath: String, topic:String,partitionId:String,offsetNum:String): Unit = {

      var zkClient=ZKPool.getZKClient(zkClientUrl,sessionTimeout, connectionTimeout)
      //kafka/consumers/groupid/offsets/topic/分区
      var new_zkOffsetPath =  zkOffsetPath +"/" + topic+"/" + partitionId
      ZkUtils.updatePersistentPath(zkClient, new_zkOffsetPath, offsetNum)
      log.warn(" 保存的偏移量topic: "+topic+"  分区"+partitionId+":"+offsetNum)
  }






  class Stopwatch {
    private val start = System.currentTimeMillis()
    def get():Long = (System.currentTimeMillis() - start)
  }






}
