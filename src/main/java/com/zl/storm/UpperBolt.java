package com.zl.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 *  一个bolt就是一个逻辑处理单元，需要继承BaseBasicBolt
 *
 *  小写字母转大写字母bolt
 */
public class UpperBolt extends BaseBasicBolt{


	//业务处理逻辑，该方法被不断的重复调用，调用一次就出来接收到的消息，在进行封装，发送到下一个组件
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		//先获取到上一个组件传递过来的数据,数据在tuple里面，根据spout发送过来的消息取value。 可以通过脚标取,也可以通过filed取
		String godName = tuple.getString(0);

		//将商品名转换成大写
		String godName_upper = godName.toUpperCase();

		//将转换完成的商品名发送出去
		collector.emit(new Values(godName_upper));

	}



	//声明该bolt组件要发出去的tuple的字段
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("uppername"));
	}

}
