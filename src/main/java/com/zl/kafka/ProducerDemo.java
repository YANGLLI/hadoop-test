package com.zl.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class ProducerDemo {
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("zk.connect", "strom01:2181,strom02:2181,strom03:2181");
		props.put("metadata.broker.list","strom01:9092,strom02:9092,strom03:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);

		// 第一个泛型为topic类型，第二个泛型为消息的类型。如果是其它的java对象，也需要序列化，并设置serializer的序列化配置
		Producer<String, String> producer = new Producer<String, String>(config);

		// 发送业务消息
		// 读取文件 读取内存数据库 读socket端口
		for (int i = 1; i <= 100; i++) {
			Thread.sleep(500);
			//topic为 mygirls
			producer.send(new KeyedMessage<String, String>("mygirls",
					"i said i love you baby for" + i + "times,will you have a nice day with me tomorrow"));
		}

	}
}