package com.zl.storm;

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
 * 添加后缀bolt
 */
public class SuffixBolt extends BaseBasicBolt {

    FileWriter fileWriter = null;


    //在bolt组件运行过程中只会被调用一次
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        try {
            fileWriter = new FileWriter("/home/hadoop/stormoutput/" + UUID.randomUUID());

            // 如果要连接数据库，或者redis，在这个地方初始化好，在execute（）方法中使用

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    //该bolt组件的核心处理逻辑
    //每收到一个tuple消息，就会被调用一次
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        //先拿到上一个组件发送过来的商品名称
        String upper_name = tuple.getString(0);

        //为上一个组件发送过来的商品名称添加后缀
        String suffix_name = upper_name + "_itisok";

        try {
            fileWriter.write(suffix_name);
            fileWriter.write("\n");
            fileWriter.flush();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }


    //本bolt已经不需要发送tuple消息到下一个组件，所以不需要再声明tuple的字段
    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {


    }

}
