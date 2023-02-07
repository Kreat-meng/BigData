package com.atguigu.reduceJion;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text,Text,TableBean> {


    private String fileName;
    private Text outK = new Text();
    private TableBean outV = new TableBean();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        if (fileName.contains("order")){
            String[] splits = line.split("\t");
            outK.set(splits[1]);
            outV.setId(splits[0]);
            outV.setPid(splits[1]);
            outV.setNum(Integer.parseInt(splits[2]));
            outV.setName("");
            outV.setFalg("order");

        }else {
            String[] s = line.split(" ");
            outK.set(s[0]);
            outV.setId("");
            outV.setPid(s[0]);
            outV.setNum(0);
            outV.setName(s[1]);
            outV.setFalg("pd");
        }

        context.write(outK,outV);
    }

}
