package com.zl.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Created by jacky on 2016/9/24.
 */
public class HdfsUtil {

    private FileSystem fs;

    @Before
    public void init() throws Exception {
        // 读取classpath下的xxx-site.xml文件，并解析其内容，封装到conf对象中。
        Configuration conf = new Configuration();

        // 也可以在代码中对conf中配置的信息进行手动配置，会覆盖配置文件中读取的值。
        conf.set("fs.defaultFS", "hdfs://192.168.43.128:9000/");

        // 根据配置信息，获取一个具体的文件系统的客户端操作对象。
        fs = FileSystem.get(new URI("hdfs://192.168.43.128:9000/"),conf,"hadoop");
    }

    /**
     * 上传文件（比较底层的实现）
     */
    @Test
    public void upload() throws IOException {

        // 会报错，因为权限不够，要么这是vm参数：-DHADOOP_USER_NAME=hadoop，要么修改aa文件夹的权限为所有人都能访问
        Path dest = new Path("hdfs://192.168.43.128:9000/aa/qingshu.txt");
        FSDataOutputStream os = fs.create(dest);

        FileInputStream in = new FileInputStream("c:/qingshu.txt");

        IOUtils.copy(in, os);
    }

    /**
     * 上传文件方法2
     * @throws IOException
     */
    @Test
    public void upload2() throws IOException {
        Path src = new Path("c:/qingshu2.txt");
        Path dest = new Path("hdfs://192.168.43.128:9000/aa/qingshu2.txt");
        fs.copyFromLocalFile(src, dest);

    }

    /**
     * 下载文件
     */
    @Test
    public void download() throws IOException {
        Path dest = new Path("c:/qingshu2.txt");
        Path src = new Path("hdfs://192.168.43.128:9000/aa/qingshu2.txt");
        fs.copyToLocalFile(src,dest);
    }

    /**
     * 获取文件列表
     */
    @Test
    public void listFiles() throws IOException {

        // listFiles 列出文件（不列出文件夹），自带递归参数
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
        while (iterator.hasNext()){
            LocatedFileStatus file = iterator.next();
            System.out.println(file.getPath().getName());
        }
        System.out.println("----------------------------------------");

        // listStatus 列出文件夹和文件，不带递归参数
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for(FileStatus fileStatu : fileStatuses){
            if(fileStatu.isDirectory()){

            }
            System.out.println(fileStatu.getPath().getName());
        }
    }

    /**
     * 创建文件夹
     */
    @Test
    public void mkdir() throws IOException {
        fs.mkdirs(new Path("/aa/bbb/ccc"));
    }

    /**
     * 删除
     */
    @Test
    public void rm() throws IOException {
        // true 表示递归删除
        fs.delete(new Path("/aa"),true);
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://192.168.43.128:9000");

        FileSystem fs = FileSystem.get(conf);

        FSDataInputStream is = fs.open(new Path("/jdk-7u65-linux-i586.tar.gz"));

        FileOutputStream os = new FileOutputStream("c:/jdk7.tgz");

        IOUtils.copy(is,os);





    }
}
