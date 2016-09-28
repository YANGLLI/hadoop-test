package com.zl.hadoop.mr.flowsum;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jacky on 2016/9/28.
 *
 * 流量求和-mapper
 *
 */
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean>{

    /**
     * 拿到日志中的一行数据，切分各个字段，抽取出我们需要的字段：手机号，上行流量，下行流量，然后分装成k-v发送出去
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 拿一行数据
        String line = value.toString();
        // 切分成各个字段
        String[] fileds = StringUtils.split(line, "\t");

        // 拿到需要的字段
        String phoneNumber = fileds[1];
        Long upFlow = Long.parseLong(fileds[7]);
        Long downFlow = Long.parseLong(fileds[8]);

        // 封装数据为key-value输出
        context.write(new Text(phoneNumber),new FlowBean(upFlow,downFlow,phoneNumber));

    }
}
