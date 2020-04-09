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
      //创建zkClient注意最后一个参数最好是ZKStringSerializer类型的，不然写进去zk里面的偏移量是乱码
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
