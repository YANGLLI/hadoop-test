package com.zl.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Created by jacky on 2016/10/29.
 */
public class HbaseDao {


    public static void main(String[] args) throws Exception {

        Configuration conf =  HBaseConfiguration.create();

        /**
         * 注意：
         * ① 只需要设置zk 的地址： 因为hbase的寻址机制是 zk--> root table --> meta table -->用户数据表
         * ② 因为habse server的配置文件里面，指定的zk的地址是 hostname:port的方式，这里需要写成主机名hadoop0X，不能写成ip地址，否则会阻塞
         */
        conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");

        // 这里的admin就相当于一个hbase的client
        HBaseAdmin admin = new HBaseAdmin(conf);

        // 创建一个表名
        TableName name = TableName.valueOf("girls");

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
