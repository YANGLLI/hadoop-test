package com.zl.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerDemo {
	private static final String topic = "mygirls";
	private static final Integer threads = 1;

	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put("zookeeper.connect", "strom01:2181,strom02:2181,strom03:2181");

		// 设置消费者组，不同的消费者组不会消费相同的消息
		props.put("group.id", "1111");

		// 设置起始偏移量  smallest表示将offset设置到起偏移量
		props.put("auto.offset.reset", "smallest");

		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector consumer =Consumer.createJavaConsumerConnector(config);

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

		// 可以在map里面设置很多个topic，如果设置了多个topic，就会消费多个topic的消息
		topicCountMap.put(topic, threads);
		// topicCountMap.put("mygirls", 1);
		// topicCountMap.put("myboys", 1);

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		
		for(final KafkaStream<byte[], byte[]> kafkaStream : streams){
			new Thread(new Runnable() { // 这里可以开启一个线程去消费消息
				@Override
				public void run() {
					for(MessageAndMetadata<byte[], byte[]> mm : kafkaStream){
						String msg = new String(mm.message());
						System.out.println(msg);
					}
				}
			
			}).start();

		}
	}
}