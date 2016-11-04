package com.zl.llyy.mr.topkurl;

import com.zl.llyy.mr.bean.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;


public class TopkURLReducer extends Reducer<Text, FlowBean, Text, LongWritable>{

	// 内存treeMap，用于排序流量
	private TreeMap<FlowBean,Text> treeMap = new TreeMap<>();  // 定义一个成员变量

	private double globalCount = 0;


	// <url,{bean,bean,bean,.......}>
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values,Context context)
			throws IOException, InterruptedException {
		Text url = new Text(key.toString());
		long up_sum = 0;
		long d_sum = 0;
		for(FlowBean bean : values){

			up_sum += bean.getUp_flow();
			d_sum += bean.getD_flow();
		}

		FlowBean bean = new FlowBean("", up_sum, d_sum);

		//每求得一条url的总流量，就累加到全局流量计数器中，等所有的记录处理完成后，globalCount中的值就是全局的流量总和
		globalCount += bean.getS_flow();

		// 如果map中存在，就去更新，没有就添加
		treeMap.put(bean,url);

	}


	//cleanup方法是在reduer任务即将退出时被调用一次
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {

		Set<Entry<FlowBean, Text>> entrySet = treeMap.entrySet();
		double tempCount = 0;

		for(Entry<FlowBean, Text> ent: entrySet){

			if(tempCount / globalCount < 0.8){

				context.write(ent.getValue(), new LongWritable(ent.getKey().getS_flow()));
				tempCount += ent.getKey().getS_flow();

			}else{
				return;
			}


		}



	}
}
