package com.zl.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by jacky on 2016/10/7.
 *
 * 集群下的java-api
 */
public class HdfsUtilHA {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        // 设置nameservice,应该把配置文件hdfs-site.xml, core-site.xml 拷贝到classpath路径下，这样就能知道ns1表示的是什么

        FileSystem fs = FileSystem.get(new URI("hdfs://ns1/"),conf,"hadoop");

        fs.copyFromLocalFile(new Path("c:/test.txt"),new Path("hdfs://ns1/"));
    }
}
