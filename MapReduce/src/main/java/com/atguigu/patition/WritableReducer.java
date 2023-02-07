package com.atguigu.patition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WritableReducer extends Reducer<Text,WritableBean,Text,WritableBean> {

    private WritableBean outv = new WritableBean();
    @Override
    protected void reduce(Text key, Iterable<WritableBean> values, Context context) throws IOException, InterruptedException {

        long upflowF = 0;
        long downflowF = 0;
        for (WritableBean value : values) {

            upflowF += value.getUpFlow();
            downflowF += value.getDownFlow();
        }
        outv.setUpFlow(upflowF);
        outv.setDownFlow(downflowF);
        outv.setSumFlow();

        context.write(key,outv);
    }
}
