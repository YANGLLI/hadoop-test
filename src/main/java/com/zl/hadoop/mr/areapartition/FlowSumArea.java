package com.zl.hadoop.mr.areapartition;

import com.zl.hadoop.mr.flowsum.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
 * Created by jacky on 2016/9/28.
 *
 * 对流量原始日志进行流量统计，将不同省份的用户统计结果输出到不同的文件，需要自定义两个机制：
 * ① 改造分区的逻辑，自订一个partitioner
 * ② 自定义reducer task的自定义并发任务数
 */
public class FlowSumArea {

    public static class FlowSumAreaMapper extends Mapper<LongWritable,Text,Text,FlowBean>{

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

    public static class FlowSumAreaReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

            long upFlow_counter = 0;
            long downFlow_counter = 0;
            for(FlowBean bean : values){
                upFlow_counter += bean.getUpFlow();
                downFlow_counter += bean.getDownFlow();
            }

            context.write(key,new FlowBean(upFlow_counter,downFlow_counter,key.toString()));
        }
    }

    /**
     *  上传jar包，执行命令：  hadoop jar hadoop-test.jar com.zl.hadoop.mr.areapartition.FlowSumArea /flow/data /flow/area-output
     *  会在/flow/area-output 文件夹下生成6个文件： part-r-00000 part-r-00001 part-r-00002 part-r-00003 part-r-00004 part-r-00005
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowSumArea.class);
        job.setMapperClass(FlowSumArea.FlowSumAreaMapper.class);
        job.setReducerClass(FlowSumArea.FlowSumAreaReducer.class);

        // 设置自定义的分组逻辑
        job.setPartitionerClass(AreaPartitioner.class);

        // 设置reduce的任务并发数，应该跟AreaPartitioner分组的数量保持一直
        // 如果设置的并发数大于 分组的数 就会输出空文件，如果设置的并发数小于 分组数，就会报错，设置多少并发数就会输出多少个文件
        // 如果设置的并发数为1（默认就是一个reduce的并发量），不报错，分组就会无效，所有结果输出到一个文件
        job.setNumReduceTasks(6);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // map和reduce一致，可以省略
        //job.setMapOutputKeyClass(FlowBean.class);
        //job.setMapOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)? 0:1);
    }
}
