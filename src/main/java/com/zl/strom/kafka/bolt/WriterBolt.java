package com.zl.strom.kafka.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
/**
 * 将数据写入文件
 * @author jacky
 *
 */
public class WriterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -6586283337287975719L;
	
	private FileWriter writer = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			//writer = new FileWriter("c:\\storm-kafka\\" + "wordcount"+UUID.randomUUID().toString());
			writer = new FileWriter("/home/hadoop/stormoutput/" + "wordcount"+UUID.randomUUID().toString());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String s = input.getString(0);
		try {
			writer.write(s);
			writer.write("\n");
			writer.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
