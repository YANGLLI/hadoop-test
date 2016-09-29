package com.zl.hadoop.mr.inverseindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by jacky on 2016/9/29.
 *
 * 实际开发中，可以通过shell脚本去配置，多个job执行顺序。
 */
public class TwoJobsRunner {

    // 多个job在一个main函数下执行
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf);

        Job job2 = Job.getInstance(conf);

        // 省略配置项....

        // 这种方式不推荐，用shell脚本去组织
        Boolean flag = job1.waitForCompletion(true);
        if(flag){
            job2.waitForCompletion(true);
        }

    }
}
