import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient

object ZKPool{
  private val map = scala.collection.mutable.Map[String, ZkClient]()
  def getZKClient(zkUrl:String): ZkClient = {
    if(map.contains(zkUrl)){
      map.get(zkUrl).get
    }else{
      val zkClient = new ZkClient(zkUrl, 30000, 30000,ZKStringSerializer)
      map.put(zkUrl, zkClient)
      zkClient
    }
  }

  def close(zkUrl:String){
    if(map.contains(zkUrl)){
      map.remove(zkUrl).get.close()
    }
  }

  def closeAll(){
    map.iterator.foreach(kv => {
      close(kv._1)
    })
  }
}