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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by jacky on 2016/9/28.
 *
 * 创建索引第一步：
 */
public class InverseIndexStepOne {

    public static class StepOneMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 拿到第一行数据
            String line = value.toString();

            // 切分出各个单词
            String[] fields = StringUtils.split(line, " ");

            // 首先获取切片信息
            FileSplit inputSplit = (FileSplit) context.getInputSplit();

            // 从文件切片中获取文件名称
            String filename = inputSplit.getPath().getName();

            // 封装k-v输出  key为： hello-->a.txt , value为1
            for(String field : fields){
                context.write(new Text(field+"-->"+filename),new LongWritable(1));
            }

        }
    }


    public static class StepOneReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

        /**
         *  拿到的数据就是<hello-->a.txt,1,1,1,1...>
         */
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long counter = 0;
            for (LongWritable value : values){
                counter += value.get();
            }

            context.write(key,new LongWritable(counter));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job jobOne = Job.getInstance(conf);

        // 设置整个job所用的那些类在哪个jar包
        jobOne.setJarByClass(InverseIndexStepOne.class);

        /**
         * 描述本job使用的map和reducer
         */
        jobOne.setMapperClass(StepOneMapper.class);
        jobOne.setReducerClass(StepOneReducer.class);

        // 指定reduce的输出数据key-value类型
        jobOne.setOutputKeyClass(Text.class);
        jobOne.setOutputValueClass(LongWritable.class);

        // 指定map的输出key-value类型
        //jobOne.setMapOutputKeyClass(Text.class);
        //jobOne.setMapOutputValueClass(LongWritable.class);

        // 指定要处理的输入的原始数据存放路径,指定一个父目录就可以了，该目录下的所有文件都会统计
        FileInputFormat.setInputPaths(jobOne,new Path(args[0]));

        // 指定处理结果的输出数据存放路径，如果已存在，先删除
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){ // 如果文件夹存在，递归删除
            fs.delete(outputPath,true);
        }
        FileOutputFormat.setOutputPath(jobOne,outputPath);

        // 将job提交给集群运行，参数表示是否监控和打印job日志
        System.exit(jobOne.waitForCompletion(true) ? 0 : 1);

    }
}
