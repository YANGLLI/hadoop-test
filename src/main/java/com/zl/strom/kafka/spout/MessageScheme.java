package com.zl.strom.kafka.spout;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 *  tuple消息字段定义，指定消息的格式
 */
public class MessageScheme implements Scheme {

    private static final long serialVersionUID = 8423372426211017613L;


    // 定义消息中字段的字段值
    @Override
    public List<Object> deserialize(byte[] bytes) {
        try {
            String msg = new String(bytes, "UTF-8");
            return new Values(msg);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 定义的字段为，msg，封装在tuple中
    @Override
    public Fields getOutputFields() {
        return new Fields("msg");
    }

}