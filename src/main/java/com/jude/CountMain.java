package com.jude;

/**
 * Created by Jude on 2014-04-10.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CountMain {

    public static void main (String[] args) throws Exception {



        Configuration conf=new Configuration();

        if(args.length!=2){
            System.out.println("Usage: <input> <output>");
            System.exit(2);
        }

        //Job 이름 설정.

        Job job=new Job(conf,"CountMain");

        //입 출력 데이터 경로 설정.

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        //Set Job Class.
        job.setJarByClass(CountMain.class);

        //Set Mapper class.
        job.setMapperClass(CountMapper.class);

        //Set Reducer class.
        job.setReducerClass(CountReducer.class);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 출력키 및 출력값 유형 설정
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);



        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);

    }//end of main




}
