package com.jude;

/**
 * Created by Jude on 2014-04-13.
 */
import java.io.*;

import java.util.StringTokenizer;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;



public class DataStore {
    public static void main(String[] args){

        try{
            FileReader fr = new FileReader(args[0]);
            BufferedReader br = new BufferedReader(fr);
            String str;


            //Connect to MongoDB
            MongoClient mongo=new MongoClient("localhost",27017);

            DB db=mongo.getDB("test");

            DBCollection table=db.getCollection("Result");


            //insert
            BasicDBObject document;


            while((str=br.readLine())!=null){

                document=new BasicDBObject();
                StringTokenizer string=new StringTokenizer(str);

                String keyword=string.nextToken();
                int count=Integer.parseInt(string.nextToken());
                document.put("Keyword",keyword);
                document.put("Number",count);
                table.insert(document);

            }




            System.out.println("Finish");
        }catch(Exception e){

            e.printStackTrace();
        }
    }
}
