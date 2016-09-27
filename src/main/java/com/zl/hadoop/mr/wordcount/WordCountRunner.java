package com.zl.hadoop.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by jacky on 2016/9/27.
 *
 * ① 用来描述一个特定的作业，比如，该作业是用哪个类作为逻辑处理中的map，哪个作为reduce
 * ② 可以指定该作业要处理的数据所在的路径
 * ③ 还可以指定作业输出的结果放到哪个路径
 * ④ 等等
 *
 */
public class WordCountRunner {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job wcjob = Job.getInstance(conf);

        // 设置整个job所用的那些类在哪个jar包
        wcjob.setJarByClass(WordCountRunner.class);

        /**
         * 描述本job使用的map和reducer
         */
        wcjob.setMapperClass(WordCountMapper.class);
        wcjob.setReducerClass(WordCountReducer.class);

        // 指定reduce的输出数据key-value类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(LongWritable.class);

        // 指定map的输出key-value类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(LongWritable.class);

        // 指定要处理的输入的原始数据存放路径,指定一个父目录就可以了，该目录下的所有文件都会统计
        FileInputFormat.setInputPaths(wcjob,new Path("/wc/srcdata/"));

        // 指定处理结果的输出数据存放路径
        FileOutputFormat.setOutputPath(wcjob,new Path("/wc/output"));

        // 将job提交给集群运行，参数表示是否打印处理过程
        wcjob.waitForCompletion(true);
    }
}
