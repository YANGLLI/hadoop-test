package com.zl.hadoop.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jacky on 2016/9/28.
 */
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean>{

    /**
     * 框架每传递一组数据<13790835188,{flowbean,flowbean,flowbean,....}>,就调用一次reduce方法
     * reduce中的业务逻辑，就是遍历values，然后累加求和再输出
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long upFlow_counter = 0;
        long downFlow_counter = 0;
        for(FlowBean bean : values){
            downFlow_counter += bean.getDownFlow();
            upFlow_counter += bean.getUpFlow();
        }

        // 直接将bean写入文件，会调用该bean的toString() 方法
        context.write(key,new FlowBean(upFlow_counter,downFlow_counter,key.toString()));

    }
}
