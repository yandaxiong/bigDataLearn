package com.xiong.mapreduce.table;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TableReduce extends Reducer<Text, TableBean, TableBean, NullWritable> {



    //注意  values循环一次会将其内存设置成垃圾内存，所以要将其复制出去
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // 0 准备存储数据的缓存
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        String pName = "";

        for (TableBean bean : values) {
            String flag = bean.getFlag();
            if ("1".equals(flag)) {
                pName = bean.getPname();
            }
            if ("0".equals(flag)) {
                TableBean orBean = new TableBean();
                try {
                    BeanUtils.copyProperties(orBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                orderBeans.add(orBean);
            }
        }

        for (TableBean bean : orderBeans) {
            System.out.println(bean);
            bean.setPname(pName);
            // 写出
            context.write(bean, NullWritable.get());
        }


    }
}
