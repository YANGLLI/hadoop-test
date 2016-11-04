package com.zl.strom.kafka.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.zl.strom.kafka.bolt.WordSpliter;
import com.zl.strom.kafka.bolt.WriterBolt;
import com.zl.strom.kafka.spout.MessageScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * storm和kafka整合
 */
public class KafkaTopo {

    public static void main(String[] args) throws Exception {

        // 设置主题
        String topic = "mygirls";

        // zk中的根目录，用来存放kafka-storm整合的数据
        String zkRoot = "/kafka-storm";
        //String zkRoot = "/storm";

        String spoutId = "KafkaSpout";

        // kafka-storm整合，storm作为kafka的消费者，是不知道消息在哪台机器上面的，所以直接找zk就可以了
        BrokerHosts brokerHosts = new ZkHosts("strom01:2181,strom02:2181,strom03:2181");

        // 定义spoutConfig
        // 第一个参数hosts是上面定义的brokerHosts
        // 第二个参数topic是该Spout订阅的topic名称
        // 第三个参数zkRoot是存储消费的offset(存储在ZK中了),当该topology故障重启后会将故障期间未消费的message继续消费而不会丢失(可配置)
        // 第四个参数id是当前spout的唯一标识
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);

        // 读取消息从哪里开始，从最前面读取
        spoutConfig.forceFromStart = true;

        // spoutConfig.maxOffsetBehind,spoutConfig.startOffsetTime 都可以用来设置偏移量

        // 定义kafkaSpout如何解析数据,storm里面tuple消息字段定义，需要自己去实现MessageScheme，实现Message接口。
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());

        //spoutConfig.zkServers = Arrays.asList(new String[] {"strom01", "strom02", "strom03"});
        //spoutConfig.zkPort = 2181;


        TopologyBuilder builder = new TopologyBuilder();

        //设置一个spout用来从kaflka消息队列中读取数据并发送给下一级的bolt组件，此处用的spout组件并非自定义的，而是storm中已经开发好的KafkaSpout
        builder.setSpout(spoutId, new KafkaSpout(spoutConfig));
        builder.setBolt("word-spilter", new WordSpliter()).shuffleGrouping(spoutId);
        // 按照word-spilter这个bolt中定义的字段，进行分组接收
        builder.setBolt("writer", new WriterBolt(), 4).fieldsGrouping("word-spilter", new Fields("word"));

        Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setNumAckers(0);
        conf.setDebug(true);


        //LocalCluster用来将topology提交到本地模拟器运行，方便开发调试
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("sufei-topo", conf, builder.createTopology());

        //提交topology到storm集群中运行
        StormSubmitter.submitTopology("sufei-topo", conf, builder.createTopology());
    }

}
