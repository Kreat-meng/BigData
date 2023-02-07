package com.atguigu.reduceJion;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.htrace.shaded.fasterxml.jackson.databind.util.BeanUtil;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class JoinReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        ArrayList<TableBean> orderList = new ArrayList<>();
        TableBean pdbean = new TableBean();


        for (TableBean value : values) {

            if ("order".equals(value.getFalg())){

                try {
                    TableBean orderBean = new TableBean();
                    BeanUtils.copyProperties(orderBean,value);
                    orderList.add(orderBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }else {
                try {
                    BeanUtils.copyProperties(pdbean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (TableBean tableBean : orderList) {

            tableBean.setName(pdbean.getName());
            context.write(tableBean,NullWritable.get());
        }
    }
}
