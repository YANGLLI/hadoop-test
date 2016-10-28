package com.zl.hadoop.mr.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jacky on 2016/9/27.
 */

/**
 * ① Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 *
 * ② reducer的输入应该和mapper的输出类型对应。
 *
 * ③ mapreduce程序，默认输入为hdfs文件类型，输出也为hdfs文件类型
 */

public class WordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

    /**
     * 框架在map处理完成之后，将所有key-value对缓存起来，进行分组（相同的key为一组 ，<key-values>），然后传递一个组，调用一次reduce方法。
     * <hello,{1,1,1,1....}>
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long count = 0;

        // 遍历value的list,进行累加求和
        for(LongWritable value:values){
            count += value.get();
        }

        // 输出这一个单词的统计结果.
        context.write(key,new LongWritable(count));

    }
}
