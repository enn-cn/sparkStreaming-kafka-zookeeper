package cn.fengsong97.tool;

import java.io.Serializable;

/**
 * Created by Administrator on 2018/7/26.
 */
public class PropertiesInfo implements Serializable {
    private static final long serialVersionUID = 2576641780956123044L;
    /**
     * 程序名称
     */
    private String programname;
    /**
     * kafka对应的topic
     */
    private String topicStr;
    /**
     * 消费kafka对应的分组
     */
    private String groupid;
    /**
     * 消费kafkakafka对应offset类型
     */
    private String offsetRest;
    /**
     * zk链接超时时间
     */
    private String zkconntimeout;
    /**
     * spark的app名称
     */
    private String appname;
    /**
     * 单个批次时间
     */
    private int times;
    /**
     * kafka的地址
     */
    private String brokerlist;
    /**
     * zookeeper地址
     */
    private String zookeeperCon;
    /**
     * 写入opentsdb的路径
     */
    private String url;
    /**
     * http请求socket超时时间
     */
    private int sockettimeout;
    /**
     * http请求connect超时时间
     */
    private int connecttimeout;
    /**
     * http请求超时时间
     */
    private int requesttimeout;
    /**
     * 保存数据失败时报警的类型
     */
    private String task_type;
    /**
     * 在试图确定某个partition的leader
     * 是否失去他的leader地位之前，
     * 需要等待的backoff时间
     */
    private String refresh_backofftimes;
    /**
     * 报警等级
     */
    private String alarm_level;
    /**
     * 发送短信的标题
     */
    private String sentitle;
    /**
     * 大的业务域
     */
    private String domain;
    /**
     * 报警电话标题
     */
    private String descstr;
    /**
     * 是否需要报警
     */
    private String isSendWarn;
    /**
     * 报警接收人
     */
    private String usernames;
    /**
     * 报警电话列表
     */
    private String tels;
    /**
     * 报警邮箱列表
     */
    private String emails;
    /**
     * Kerberos登录用户
     */
    private String kerberosusername;
    /**
     * hdfs存储路径
     */
    private String hdfsfile;
    /**
     * failtimes失败指定次数后终止程序
     */
    private int failtimes;
    /**
     * 写入opentsdb字符串的最大长度
     */
    private int out_batch_size;
    /**
     * 写入opentsdb数组中最大元素个数
     */
    private int out_batch_length;
    /**
     * 保存所有异常数据的topic
     */
    private String errorDataTopic;


    public PropertiesInfo() {
        PropertiesUtil util = new PropertiesUtil("/common.properties");
        this.out_batch_size = util.getProperty("out_batch_size") == null ? 8192 : (Integer.parseInt(util.getProperty("out_batch_size")) >= 8192 ? 8192 : Integer.parseInt(util.getProperty("out_batch_size")));
        this.out_batch_length = util.getProperty("out_batch_length") == null ? 40 : Integer.parseInt(util.getProperty("out_batch_length"));
        brokerlist = util.getProperty("brokerlist") == null ? "" : util.getProperty("brokerlist");
        errorDataTopic = util.getProperty("errorDataTopic") == null ? "" : util.getProperty("errorDataTopic");
    }

    public PropertiesInfo(String prfix, String domain, String programname) {
        PropertiesUtil util = new PropertiesUtil("/common.properties");
        topicStr = util.getProperty(prfix + "_topic") == null ? "" : util.getProperty(prfix + "_topic");
        groupid = util.getProperty(prfix + "_groupid") == null ? "" : util.getProperty(prfix + "_groupid");
        offsetRest = util.getProperty(prfix + "_offsetRest") == null ? "smallest" : util.getProperty(prfix + "_offsetRest");
        appname = util.getProperty(prfix + "_appname") == null ? "" : util.getProperty(prfix + "_appname");
        times = Integer.parseInt(util.getProperty(prfix + "_times") == null ? "3" : util.getProperty(prfix + "_times"));
        brokerlist = util.getProperty("brokerlist") == null ? "" : util.getProperty("brokerlist");
        zookeeperCon = util.getProperty("zookeeperCon") == null ? "" : util.getProperty("zookeeperCon");
        url = util.getProperty(prfix + "_puturl") == null ? "" : util.getProperty(prfix + "_puturl");
        sockettimeout = Integer.parseInt(util.getProperty("sockettimeout") == null ? "1000" : util.getProperty("sockettimeout"));
        connecttimeout = Integer.parseInt(util.getProperty("connecttimeout") == null ? "1000" : util.getProperty("connecttimeout"));
        requesttimeout = Integer.parseInt(util.getProperty("requesttimeout") == null ? "1000" : util.getProperty("requesttimeout"));
        refresh_backofftimes = util.getProperty("refresh_backofftimes") == null ? "3000" : util.getProperty("refresh_backofftimes");
        task_type = util.getProperty("task_type") == null ? "" : util.getProperty("task_type");
        alarm_level = util.getProperty("alarm_level") == null ? "" : util.getProperty("alarm_level");
        sentitle = util.getProperty("sentitle") == null ? "" : util.getProperty("sentitle");
        isSendWarn = util.getProperty("isSendWarn") == null ? "" : util.getProperty("isSendWarn");
        descstr = util.getProperty(prfix + "_descstr") == null ? "" : util.getProperty(prfix + "_descstr");
        usernames = util.getProperty("usernames") == null ? "" : util.getProperty("usernames");
        tels = util.getProperty("tels") == null ? "" : util.getProperty("tels");
        emails = util.getProperty("emails") == null ? "" : util.getProperty("emails");
        kerberosusername = util.getProperty("kerberosusername") == null ? "" : util.getProperty("kerberosusername");
//        hdfsfile = util.getProperty("hdfsfile") == null ? "" : util.getProperty("hdfsfile");
        hdfsfile = util.getProperty(prfix + "_hdfsfile") == null ? "" : util.getProperty(prfix + "_hdfsfile");
        zkconntimeout = util.getProperty("zkconntimeout") == null ? "" : util.getProperty("zkconntimeout");
        failtimes = util.getProperty("failtimes") == null ? 2 : Integer.parseInt(util.getProperty("failtimes"));
        errorDataTopic = util.getProperty("errorDataTopic") == null ? "" : util.getProperty("errorDataTopic");
        this.domain = domain;
        this.programname = programname;
    }

    public String getTopicStr() {
        return topicStr;
    }

    public String getGroupid() {
        return groupid;
    }

    public String getOffsetRest() {
        return offsetRest;
    }

    public String getAppname() {
        return appname;
    }

    public int getTimes() {
        return times;
    }

    public String getBrokerlist() {
        return brokerlist;
    }

    public String getZookeeperCon() {
        return zookeeperCon;
    }

    public String getUrl() {
        return url;
    }

    public int getSockettimeout() {
        return sockettimeout;
    }

    public int getConnecttimeout() {
        return connecttimeout;
    }

    public int getRequesttimeout() {
        return requesttimeout;
    }

    public String getTask_type() {
        return task_type;
    }

    public String getAlarm_level() {
        return alarm_level;
    }

    public String getSentitle() {
        return sentitle;
    }

    public String getDomain() {
        return domain;
    }

    public String getDescstr() {
        return descstr;
    }

    public String getIsSendWarn() {
        return isSendWarn;
    }

    public String getUsernames() {
        return usernames;
    }

    public String getTels() {
        return tels;
    }

    public String getEmails() {
        return emails;
    }

    public String getProgramname() {
        return programname;
    }

    public String getKerberosusername() {
        return kerberosusername;
    }

    public String getHdfsfile() {
        return hdfsfile;
    }

    public String getZkconntimeout() {
        return zkconntimeout;
    }

    public int getFailtimes() {
        return failtimes;
    }

    public int getOut_batch_size() {
        return out_batch_size;
    }

    public int getOut_batch_length() {
        return out_batch_length;
    }

    public String getErrorDataTopic() {
        return errorDataTopic;
    }

    public String getRefresh_backofftimes() {
        return refresh_backofftimes;
    }
}
