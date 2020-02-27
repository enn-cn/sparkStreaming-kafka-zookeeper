package cn.fengsong97.tool

import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient

object ZKPool{

  val strkey="someMapKey"
  private val map = scala.collection.mutable.Map[String, ZkClient]()

  def getZKClient(zkUrl:String, sessionTimeout: Int, connectionTimeout: Int): ZkClient = {
    if(map.contains(strkey)){
      map.get(strkey).get
    }else{
      val zkClient = new ZkClient(zkUrl, sessionTimeout, connectionTimeout,ZKStringSerializer)
      map.put(strkey, zkClient)
      zkClient
    }
  }

  def close(zkUrl:String){
    if(map.contains(strkey)){
      map.remove(strkey).get.close()
    }
  }

  def closeAll(){
    map.iterator.foreach(kv => {
      close(kv._1)
    })
  }
}
