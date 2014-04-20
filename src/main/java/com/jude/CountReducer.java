package com.jude;

/**
 * Created by Jude on 2014-04-10.
 */
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class CountReducer extends Reducer <Text,IntWritable,Text,IntWritable> {

    private IntWritable result=new IntWritable();




    @Override
    public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{

        int sum=0;

        //Mapper의 결과물로 나온 <Key,Value>의 쌍들중에서 Key값을 기준으로 Value값들을 모두 더해준다.
        for(IntWritable value:values){
        sum+=value.get();
        }

        result.set(sum);
        context.write(key,result);

        }
 }
