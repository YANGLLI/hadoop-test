package com.zl.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jacky on 2016/10/29.
 */
public class HbaseDao {


    @Test
    public void insertTest() throws IOException {

        Configuration conf =  HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");

        // HTable 对表操作客户端
        HTable nvshen = new HTable(conf, "nvshen");

        // 使用hbase提供的工具类Bytes，将string转换为byte[]
        Put name = new Put(Bytes.toBytes("rk0001")); // 设置行键

        // 添加列族，字段及字段值
        name.add(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("angelababy"));

        Put age = new Put(Bytes.toBytes("rk0001"));
        age.add(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes(18));

        List<Put> puts = new ArrayList<>();
        puts.add(name);
        puts.add(age);

        // 批量put
        nvshen.put(puts);

        //nvshen.close();

    }

    public static void main(String[] args) throws Exception {


        Configuration conf =  HBaseConfiguration.create();

        /**
         * 注意：
         * ① 只需要设置zk 的地址： 因为hbase的寻址机制是 zk--> root table --> meta table -->用户数据表
         * ② 因为habse server的配置文件里面，指定的zk的地址是 hostname:port的方式，这里需要写成主机名hadoop0X，不能写成ip地址，否则会阻塞
         **/
        conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");

        // 这里的admin就相当于一个hbase的client
        HBaseAdmin admin = new HBaseAdmin(conf);

        // 创建一个表名
        TableName name = TableName.valueOf("nvshen");

        // 创建一个表名描述器
        HTableDescriptor desc = new HTableDescriptor(name);

        // 创建两个列族
        HColumnDescriptor base_info = new HColumnDescriptor("base_info");
        HColumnDescriptor extra_info = new HColumnDescriptor("extra_info");

        // 设置最多可存储的版本数为5
        base_info.setMaxVersions(5);

        // 将列族添加到表描述器中
        desc.addFamily(base_info);
        desc.addFamily(extra_info);

        // 创建表
        admin.createTable(desc);


    }
}
