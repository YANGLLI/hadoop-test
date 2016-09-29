package com.zl.hadoop.mr.inverseindex;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by jacky on 2016/9/29.
 */
public class InverseIndexStepTwo {

    public static class StepTwoMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 拿到第一行数据
            String line = value.toString();

            // 切分,一行的内容是 hello-->a.txt  3
            String[] fields = StringUtils.split(line, "\t"); // 切分为[hello-->a.txt, 3]

            String[] wordAndFilename = StringUtils.split(fields[0],"-->");// 切分为[hello,a.txt]

            String word = wordAndFilename[0];
            String filename = wordAndFilename[1];
            long count = Long.parseLong(fields[1]);

            // 封装k-v输出  key为： hello, value为 a.txt-->3
            context.write(new Text(word),new Text(filename+"-->"+count));

        }
    }


    public static class StepTwoReducer extends Reducer<Text,Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // 拿到的数据：<hello,{a.txt-->3,b.txt-->2,c.txt-->1}>
            StringBuffer sb = new StringBuffer();
            for(Text value:values){
                sb.append(" ").append(value);
            }
            context.write(key,new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job jobTwo = Job.getInstance(conf);

        // 设置整个job所用的那些类在哪个jar包
        jobTwo.setJarByClass(InverseIndexStepOne.class);

        /**
         * 描述本job使用的map和reducer
         */
        jobTwo.setMapperClass(StepTwoMapper.class);
        jobTwo.setReducerClass(StepTwoReducer.class);

        // 指定reduce的输出数据key-value类型
        jobTwo.setOutputKeyClass(Text.class);
        jobTwo.setOutputValueClass(Text.class);

        // 指定map的输出key-value类型
        //jobTwo.setMapOutputKeyClass(Text.class);
        //jobTwo.setMapOutputValueClass(Text.class);

        // 指定要处理的输入的原始数据存放路径,指定一个父目录就可以了，该目录下的所有文件都会统计
        FileInputFormat.setInputPaths(jobTwo,new Path(args[0]));

        // 指定处理结果的输出数据存放路径，如果已存在，先删除
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){ // 如果文件夹存在，递归删除
            fs.delete(outputPath,true);
        }
        FileOutputFormat.setOutputPath(jobTwo,outputPath);

        // 将job提交给集群运行，参数表示是否监控和打印job日志
        System.exit(jobTwo.waitForCompletion(true) ? 0 : 1);
    }
}
