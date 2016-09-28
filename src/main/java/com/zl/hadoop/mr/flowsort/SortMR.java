package com.zl.hadoop.mr.flowsort;

import com.zl.hadoop.mr.flowsum.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by jacky on 2016/9/28.
 *
 * 将Map和Reduce写在一个类里面，作为内部类使用，自定义的排序输出结果
 *
 * 这个自定义的排序，是在flowsum的完成结果之后的，二次mapreduce
 */
public class SortMR {

    public static class SortMapper extends Mapper<LongWritable,Text,FlowBean,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 拿到一行数据，切分出各个字段，封装为一个flowbean，作为key输出
            String line = value.toString();
            String[] fields = StringUtils.split(line, "\t");

            String phoneNumber = fields[0];
            Long upFlow = Long.parseLong(fields[1]);
            Long downFlow = Long.parseLong(fields[2]);

            // 输出的FlowBean实现了Comparable方法，会按照自定义的排序规则（逆序），分别调用reduce程序
            context.write(new FlowBean(upFlow,downFlow,phoneNumber),NullWritable.get());
        }
    }

    public static class SortReducer extends Reducer<FlowBean,NullWritable,Text,FlowBean>{
        @Override
        protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String phoneNumber = key.getPhoneNumber();
            context.write(new Text(phoneNumber),key);
        }
    }


    /**
     * 执行的命令 hadoop jar hadoop-test.jar com.zl.hadoop.mr.flowsum.FlowRunner  /flow/output  /flow/sort-output
     * 这次的输入路径是以flowsum的mapreduce输出结果进行二次mapreduce
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SortMR.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)? 0:1);

    }
}
