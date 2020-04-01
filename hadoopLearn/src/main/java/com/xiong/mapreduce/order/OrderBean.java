package com.xiong.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean  implements WritableComparable<OrderBean> {

    private  String orderId; //订单id
    private  Double  price;//商品价格

    @Override
    public int compareTo(OrderBean o) {

        //两次排序
        //1、订单id排序
        int  comResult = this.orderId.compareTo(o.getOrderId());

        //2、按照价格排序
        if(comResult==0){
            comResult = this.price > o.getPrice()?-1:1;
        }
        return comResult;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.price =dataInput.readDouble();
    }

    public OrderBean(String orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public OrderBean() {
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }
    @Override
    public String toString() {
        return orderId + "\t" + price;
    }

}
