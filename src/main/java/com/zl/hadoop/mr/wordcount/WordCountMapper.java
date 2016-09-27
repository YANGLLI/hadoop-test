package com.zl.hadoop.mr.wordcount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jacky on 2016/9/27.
 */

/**
 * ① Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 4个泛型中，前两个是指定mapper指定的输入数据类型，KEYIN是输入的key的类型，VALUEIN是输入的value的类型,
 *   KEYOUT是输出的key的类型，VALUEOUT是输出的value的类型. KEYOUT-VALUEOUT是输入给reduce的
 *
 * ② map和reduce的数据输入输出都是以key-value对的形式封装的
 *
 * ③ 默认情况下，框架传递给我们的mapper的输入数据中，key是要处理的文本中的一行的起始偏移量，Long类型（因为框架是读一行，就调用一次mapeer程序），把这一行的内容作
 * 为value，String类型，传入给mapper的
 *
 * ④ 我们处理的数据是在网络中传输的，因此我们定义的类型，是需要序列化的。
 *
 * ⑤ 对于jdk自己实现的序列化，有很多冗余，会增加网络负担，mapreduce实现了自己的序列化
 * Long <---> LongWritable , String <---->Text
 */

public class WordCountMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

    /**
     * mapreduce框架，每读取一行数据，就调用一次该方法，具体的调用逻辑就写在该方法中，我们业务要处理的数据已经被框架传递进来。在方法参数中key-value
     * key 是这一行数据的起始偏移量，value是这一行的文本内容
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 将这一行的内容转换为String类型
        String line = value.toString();

        // 将这一行的内容按特定的分隔符切分
        String[] words = StringUtils.split(line, " ");

        // 遍历单词数组，输出key-value形式， key：单词，value：1
        for(String word : words){
            // write方法的传入参数，是在定义Mapper泛型的时候定义好的 Text,LongWritable
            context.write(new Text(word),new LongWritable(1));
        }

    }
}
