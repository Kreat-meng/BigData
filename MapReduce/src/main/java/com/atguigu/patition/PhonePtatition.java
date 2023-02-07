package com.atguigu.patition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 	手机号136、137、138、139开头都分别放到一个独立的4个文件中，其他开头的放到一个文件中。
 */
public class PhonePtatition extends Partitioner<Text,WritableBean> {

    @Override
    public int getPartition(Text text, WritableBean writableBean, int numPartitions) {
        String s = text.toString();

        String head = s.substring(0, 3);

        if (head.equals("136")){
            return 0;
        }else if (head.equals("137")){
            return 1;
        }else if (head.equals("138")){
            return 2;
        }else if (head.equals("139")){
            return 3;
        }
        return 4;
    }
}
