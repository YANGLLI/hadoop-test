package com.zl.hadoop.mr.flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jacky on 2016/9/28.
 *
 * FlowBean 是我们自定义的一种数据类型，要在hadoop的各个节点之间传输，应该遵循hadoop序列化机制
 * 就必须实现hadoop相应的序列化接口
 */
public class FlowBean implements Writable,Comparable<FlowBean>{


    // 上行流量
    private long upFlow;

    // 下行流量
    private long downFlow;

    // 总流量
    private long sumFlow;

    //手机号
    private String phoneNumber;

    /**
     * 将对象数据序列化到流中（序列化）
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phoneNumber);
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 从数据流中反序列化对象数据（反序列化）
     * 从数据流中读出对象字段时，必须和序列化时的顺序保持一致。
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        phoneNumber = dataInput.readUTF();
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }


    /**
     * compareTo()默认升序：比你大，返回1，比你小返回-1，相等返回0；
     */
    @Override
    public int compareTo(FlowBean o) {

        // 与默认相反，就降序排序
        return sumFlow > o.getSumFlow() ? -1 : 1;
    }

    /**
     * 因为反序列化需要用到反射，所以给一个无参的构造函数
     */
    public FlowBean() {
    }

    public FlowBean(long upFlow, long downFlow, String phoneNumber) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
        this.phoneNumber = phoneNumber;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}
