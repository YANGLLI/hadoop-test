package com.zl.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by jacky on 2016/10/28.
 */
public class HbaseDao {

    public static void main(String[] args) throws IOException {


        Configuration conf =  HBaseConfiguration.create();

        // 只用设置zk的位置就可以了，hbase的寻址机制是 zk --> root table --> meta table --> 用户数据表
        conf.set("hbase.zookeeper.quorum","192.168.43.131:2181,192.168.43.132:2181,192.168.43.133:2181");

        // hBaseAdmin就是一个连接client
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);

        // 新建一个表名
        TableName name = TableName.valueOf("nvshen");


        HTableDescriptor desc = new HTableDescriptor(name);


        HColumnDescriptor base_info = new HColumnDescriptor("base_info");
        HColumnDescriptor extra_info = new HColumnDescriptor("extra_info");

        base_info.setMaxVersions(5);

        desc.addFamily(base_info);
        desc.addFamily(extra_info);

        hBaseAdmin.createTable(desc);

    }
}
