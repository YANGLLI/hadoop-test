package com.zl.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * 组织各个处理组件形成一个完整的处理流程，就是所谓的topology(类似于mapreduce程序中的job)
 * 并且将该topology提交给storm集群去运行，topology提交到集群后就将永无休止地运行，除非人为或者异常退出。
 *
 * @author jacky
 */
public class TopoMain {

	/**
	 * 分析：
	 conf.setNumWorkers(4) 表示设置了4个worker来执行整个topology的所有组件

	 builder.setBolt("boltA",new BoltA(),  4)  ---->指明 boltA组件的线程数excutors总共有4个
	 builder.setBolt("boltB",new BoltB(),  4) ---->指明 boltB组件的线程数excutors总共有4个
	 builder.setSpout("randomSpout",new RandomSpout(),  2) ---->指明randomSpout组件的线程数excutors总共有2个

	 -----意味着整个topology中执行所有组件的总线程数为4+4+2=10个
	 ----worker数量是4个，有可能会出现这样的负载情况，  worker-1有2个线程，worker-2有2个线程，worker-3有3个线程，worker-4有3个线程

	 如果指定某个组件的具体task并发实例数
	 builder.setSpout("randomspout", new RandomWordSpout(), 4).setNumTasks(8);
	 ----意味着对于这个组件的执行线程excutor来说，一个excutor将执行8/4=2个task

	 *
     */
	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		//将我们的spout组件设置到topology中去
		//parallelism_hint ：4  表示用4个excutor(工作线程)来执行这个组件
		//setNumTasks(8) 设置的是该组件执行时的并发task数量，也就意味着1个excutor会运行2个task
		// 意思是四个线程，跑8个task
		builder.setSpout("randomspout", new RandomWordSpout(), 4).setNumTasks(8); // 第一个参数为id

		//将大写转换bolt组件设置到topology，并且指定它接收randomspout组件的消息
		//.shuffleGrouping("randomspout")包含两层含义：
		//1、upperbolt组件接收的tuple消息一定来自于randomspout组件
		//2、randomspout组件和upperbolt组件的大量并发task实例之间收发消息时采用的分组策略是随机分组shuffleGrouping
		builder.setBolt("upperbolt", new UpperBolt(), 4).shuffleGrouping("randomspout");

		//将添加后缀的bolt组件设置到topology，并且指定它接收upperbolt组件的消息
		builder.setBolt("suffixbolt", new SuffixBolt(), 4).shuffleGrouping("upperbolt");

		//用builder来创建一个topology
		StormTopology demotop = builder.createTopology();


		//配置一些topology在集群中运行时的参数
		Config conf = new Config();
		//这里设置的是整个demotop所占用的槽位数（slots），也就是worker的数量
		conf.setNumWorkers(4);
		conf.setDebug(true);
		// 不关心事务处理（消息丢失不处理）
		conf.setNumAckers(0);


		//将这个topology提交给storm集群运行
		StormSubmitter.submitTopology("demotopo", conf, demotop);

	}
}
