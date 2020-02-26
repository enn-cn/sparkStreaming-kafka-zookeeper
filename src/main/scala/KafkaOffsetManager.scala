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

  def readOffsets(zkClient: ZkClient, zkOffsetPath: String, topicSet: Set[String]): Map[TopicAndPartition, Long] = {
    var topicMap= ZkUtils.getPartitionsForTopics(zkClient,topicSet.toList)
    val topics = topicMap.keys


    var offsets =topics.flatMap(key=>{
      topicMap.get(key).get.toList.map(part =>{
        //循环获取每个topic和对应分区 保存的数据
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

  def saveOffsetPart(zkClientUrl: String,sessionTimeout: Int, connectionTimeout: Int, zkOffsetPath: String, topic:String,partitionId:String,offsetNum:String): Unit = {

      var zkClient=ZKPool.getZKClient(zkClientUrl,sessionTimeout, connectionTimeout)
      //kafka/consumers/groupid/offsets/topic/分区
      var new_zkOffsetPath =  zkOffsetPath +"/" + topic+"/" + partitionId
      ZkUtils.updatePersistentPath(zkClient, new_zkOffsetPath, offsetNum)
//      log.warn(" 保存的偏移量topic: "+topic+"  分区"+partitionId+":"+offsetNum)
  }






  class Stopwatch {
    private val start = System.currentTimeMillis()
    def get():Long = (System.currentTimeMillis() - start)
  }






}
