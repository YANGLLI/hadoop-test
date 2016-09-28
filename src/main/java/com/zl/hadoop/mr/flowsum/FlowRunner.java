package com.zl.hadoop.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by jacky on 2016/9/28.
 *
 * 这是job描述和提交类的规范写法，不规范的写法就是直接把run方法的逻辑丢到main方法中去
 */
public class FlowRunner extends Configured implements Tool{
    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowRunner.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        FileInputFormat.setInputPaths(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));

        return job.waitForCompletion(true) ? 0 : 1;

    }

    /**
     * 在集群下运行的命令为(这次的main方法需要带参数)：  hadoop jar hadoop-test.jar com.zl.hadoop.mr.flowsum.FlowRunner  /flow/data  /flow/output
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FlowRunner(), args);
        System.exit(res);
    }
}
