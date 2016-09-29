package com.zl.hadoop.mr.areapartition;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * Created by jacky on 2016/9/28.
 */
public class AreaPartitioner<KEY,VALUE> extends Partitioner<KEY,VALUE> {

    private static HashMap<String,Integer> areaMap = new HashMap<String,Integer>();

    static {
        // 模拟数据库操作
        areaMap.put("135",0);
        areaMap.put("136",1);
        areaMap.put("137",2);
        areaMap.put("138",3);
        areaMap.put("139",4);
    }


    @Override
    public int getPartition(KEY key, VALUE value, int i) {

        // 从key中拿到手机号，查询手机归属字典，不同省份返回不同的组号（将key进行分组）
        int areaCode = areaMap.get(key.toString().substring(0, 3))==null ? 5: areaMap.get(key.toString().substring(0, 3));
        return areaCode;
    }
}
