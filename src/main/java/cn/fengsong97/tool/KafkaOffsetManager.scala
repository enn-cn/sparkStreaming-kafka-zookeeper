package cn.fengsong97.tool

import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import scala.collection.mutable

/**
  *
  * 负责kafka偏移量的读取和保存
  *
  * Created by fengsong97 on 2020年03月09日22:39:27.
  */
object KafkaOffsetManager {

  lazy val log = org.apache.log4j.LogManager.getLogger("KafkaOffsetManage")

  def readOffsets(zkClient: ZkClient, zkOffsetPath: String,
                  partitionsForTopics: mutable.Map[String, Seq[Int]]): Map[TopicAndPartition, Long] = {
    val topics = partitionsForTopics.keys
    var offsets =topics.flatMap(key=>{
      partitionsForTopics.get(key).get.toList.map(part =>{
        //循环获取每个topic和对应分区 保存的数据
        //kafka/consumers/groupid/offsets/topic/分区
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

  def saveOffsetPart(zkClientUrl: String,sessionTimeout: Int, connectionTimeout: Int,
                     zkOffsetPath: String, topic:String,partitionId:String,offsetNum:String): Unit = {

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



  /**
    * 通过consumer 获得分区数量
    * @param brokerlist
    * @param groupid
    * @param topicSet
    * @return
    */
  def getPartitionsByConsumer(brokerlist: String,
                              groupid: String,
                              topicSet:Set[String]): mutable.Map[String, Seq[Int]] ={
    val map: mutable.Map[String, Seq[Int]] = mutable.Map()
    topicSet.foreach(topic=>{
      var kafkaConsumer =KafkaConsumerTool.get(brokerlist,groupid)
      var it = kafkaConsumer.partitionsFor(topic).iterator();

      var str=""
      while (it.hasNext){
        str+= (it.next().partition().toString +",")
      }
      map.put(topic,str.split(",").filter(_!="").map(_.toInt).toSeq )
    })
    map

  }

  /**
    * 通过zk 获得分区数量
    * @param zkClient
    * @param topics
    * @return
    */
  def getPartitionsByZookeeper(zkClient: ZkClient,
                               topics: Seq[String]): mutable.Map[String, Seq[Int]] ={
    ZkUtils.getPartitionsForTopics(zkClient,topics.toList)
  }


}
