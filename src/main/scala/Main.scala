import cn.fengsong97.tool.{KerberosUtil, PropertiesInfo}

/** Creatingvalue 能源创值  */
object Creatingvalue {
  def main(args: Array[String]): Unit = {
    System.setProperty("java.security.krb5.realm", "DEV.ENN.CN")
    System.setProperty("java.security.krb5.kdc", "host243.master.dev.cluster.enn.cn")

    val prop = new PropertiesInfo("creatingvalue", "creatingvalue", "cn.enn.bi.Creatingvalue")
    KerberosUtil.loginUserFromKeytab(prop.getKerberosusername, prop.getKerberosusername + ".keytab")
    new SparkDirectStreaming(prop).begin();

  }
}