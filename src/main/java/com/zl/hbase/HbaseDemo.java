package com.zl.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class HbaseDemo {

    private Configuration conf = null;

    @Before
    public void init() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181");
    }

    @Test
    public void testDrop() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        // 首先disable，然后在delete
        admin.disableTable("account");
        admin.deleteTable("account");
        admin.close();
    }

    @Test
    public void testPut() throws Exception {
        HTable table = new HTable(conf, "person_info");
        Put p = new Put(Bytes.toBytes("person_rk_bj_zhang_000002"));
        p.add("base_info".getBytes(), "name".getBytes(), "zhangwuji".getBytes());
        table.put(p);
        table.close();
    }

    @Test
    public void testGet() throws Exception {
        HTable table = new HTable(conf, "person_info");
        Get get = new Get(Bytes.toBytes("person_rk_bj_zhang_000001"));

        // get.addColumn(); 设置返回哪些列

        get.setMaxVersions(5);
        Result result = table.get(get);
        List<Cell> cells = result.listCells();

        //result.getValue(family, qualifier);  可以从result中直接取出一个特定的value

        //遍历出result中所有的键值对
        for (KeyValue kv : result.list()) {
            String family = new String(kv.getFamily());
            System.out.println(family);
            String qualifier = new String(kv.getQualifier());
            System.out.println(qualifier);
            System.out.println(new String(kv.getValue()));

        }
        table.close();
    }

    /**
     * 多种过滤条件的使用方法，行键设计的好，过滤得就更方便
     *
     * @throws Exception
     */
    @Test
    public void testScan() throws Exception {

        // 获取htable对象
        HTable table = new HTable(conf, "person_info".getBytes());

        // 起始row，到结束row
        Scan scan = new Scan(Bytes.toBytes("person_rk_bj_zhang_000001"), Bytes.toBytes("person_rk_bj_zhang_000002"));

        //前缀过滤器----针对行键
        Filter filter = new PrefixFilter(Bytes.toBytes("person"));

        //行过滤器
        ByteArrayComparable rowComparator = new BinaryComparator(Bytes.toBytes("person_rk_bj_zhang_000001"));
        // 第一个参数，传入比较符， 小于等于"person_rk_bj_zhang_000001"这个行键的
        RowFilter rf = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, rowComparator);

        /**
         * 假设rowkey格式为：创建日期_发布日期_ID_TITLE
         * 目标：查找  发布日期  为  2014-12-21  的数据
         */
        rf = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("_2014-12-21_"));


        //单值过滤器 1 完整匹配字节数组
        new SingleColumnValueFilter("base_info".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "zhangsan".getBytes());
        //单值过滤器2 匹配正则表达式
        ByteArrayComparable comparator = new RegexStringComparator("zhang.");
        new SingleColumnValueFilter("info".getBytes(), "NAME".getBytes(), CompareFilter.CompareOp.EQUAL, comparator);

        //单值过滤器2 匹配是否包含子串,大小写不敏感
        comparator = new SubstringComparator("wu");
        new SingleColumnValueFilter("info".getBytes(), "NAME".getBytes(), CompareFilter.CompareOp.EQUAL, comparator);

        //键值对元数据过滤-----family过滤----字节数组完整匹配
        FamilyFilter ff = new FamilyFilter(
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("base_info"))   //表中不存在inf列族，过滤结果为空
        );
        //键值对元数据过滤-----family过滤----字节数组前缀匹配
        ff = new FamilyFilter(
                CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(Bytes.toBytes("inf"))   //表中存在以inf打头的列族info，过滤结果为该列族所有行
        );


        //键值对元数据过滤-----qualifier过滤----字节数组完整匹配

        filter = new QualifierFilter(
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("na"))   //表中不存在na列，过滤结果为空
        );
        filter = new QualifierFilter(
                CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(Bytes.toBytes("na"))   //表中存在以na打头的列name，过滤结果为所有行的该列数据
        );

        //基于列名(即Qualifier)前缀过滤数据的ColumnPrefixFilter
        filter = new ColumnPrefixFilter("na".getBytes());

        //基于列名(即Qualifier)多个前缀过滤数据的MultipleColumnPrefixFilter
        byte[][] prefixes = new byte[][]{Bytes.toBytes("na"), Bytes.toBytes("me")};
        filter = new MultipleColumnPrefixFilter(prefixes);

        //为查询设置过滤条件
        scan.setFilter(filter);


        scan.addFamily(Bytes.toBytes("base_info"));

        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            /**  从result中遍历
             for(KeyValue kv : r.list()){
             String family = new String(kv.getFamily());
             System.out.println(family);
             String qualifier = new String(kv.getQualifier());
             System.out.println(qualifier);
             System.out.println(new String(kv.getValue()));
             }
             */
            //直接从result中取到某个特定的value
            byte[] value = r.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("name"));
            System.out.println(new String(value));
        }
        table.close();
    }


    @Test
    public void testDel() throws Exception {
        HTable table = new HTable(conf, "user");
        Delete del = new Delete(Bytes.toBytes("rk0001"));
        del.deleteColumn(Bytes.toBytes("data"), Bytes.toBytes("pic"));
        table.delete(del);
        table.close();
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.quorum", "weekend05:2181,weekend06:2181,weekend07:2181");
        HBaseAdmin admin = new HBaseAdmin(conf);

        TableName tableName = TableName.valueOf("person_info");
        HTableDescriptor td = new HTableDescriptor(tableName);
        HColumnDescriptor cd = new HColumnDescriptor("base_info");
        cd.setMaxVersions(10);
        td.addFamily(cd);
        admin.createTable(td);

        admin.close();

    }


}
