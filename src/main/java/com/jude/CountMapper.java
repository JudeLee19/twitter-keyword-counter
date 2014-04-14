package com.jude;

/**
 * Created by Jude on 2014-04-11.
 */
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{


    //output value of mapper
    private final static IntWritable outputValue=new IntWritable(1);

    //output key of mapper
    private Text outputKey=new Text();

    private static final String Regex="벚꽃";

    private static final String Regex2="런닝맨";
    private static final String Regex3="밥";

    @Override
    public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{



        //Regex 패턴 설정.
        Pattern pattern=Pattern.compile(Regex);

        Pattern pattern2=Pattern.compile(Regex2);
        Pattern pattern3=Pattern.compile(Regex3);


        String[] line;

        if(key.get()>0) {
            Matcher matcher = pattern.matcher(value.toString());
            Matcher matcher2= pattern2.matcher(value.toString());
            Matcher matcher3=pattern3.matcher(value.toString());

            try {


                while(matcher.find()) {
                    outputKey.set("벚꽃");
                    context.write(outputKey,outputValue);
                }
                while(matcher2.find()){
                    outputKey.set("런닝맨");
                    context.write(outputKey,outputValue);
                }
                while(matcher3.find()){
                    outputKey.set("밥");
                    context.write(outputKey,outputValue);
                }


            } catch (Exception e) {
                e.printStackTrace();
            }

        }



    }//end of map.

}
