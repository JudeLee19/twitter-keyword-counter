package com.jude;

/**
 * Created by Jude on 2014-04-10.
 */
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer <Text,IntWritable,Text,IntWritable> {
    private IntWritable result=new IntWritable();
    public static  MongoClient mongo;
    public static  DB db;
    public static  DBCollection table;
    public static  BasicDBObject document;
    @Override
    public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{

        int sum=0;
        //Mapper의 결과물로 나온 <Key,Value>의 쌍들중에서 Key값을 기준으로 Value값들을 모두 더해준다.
        for(IntWritable value:values){
        sum+=value.get();
        }
        result.set(sum);
        context.write(key,result);
        String keyword=key.toString();

        //MongoDb에 Data를 저장할 외부 서버의 주소입력력 및 저장과정
        mongo=new MongoClient("newheart.kr",27017);
        db=mongo.getDB("test");
        table=db.getCollection("Result");
        document=new BasicDBObject();
        document.put("Keyword",keyword);
        document.put("Number",sum);
        table.insert(document);

        }




 }
