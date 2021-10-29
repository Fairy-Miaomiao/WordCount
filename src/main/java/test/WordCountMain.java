package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WordCountMain {
    public static void main(String args[]) throws ClassNotFoundException, IOException, InterruptedException,NullPointerException{
        //对input文件夹下的每个文件分别进行WordCount,结果保存在
        //路径
        //File input =new File("input\\");
        File input=new File("hdfs:\\hadoop-master:9000\\user\\dell\\input\\");
        //File input=new File("hdfs:\\localhost:9000\\user\\dell\\input\\");
        File[] inputArray=input.listFiles();
        System.out.println(inputArray.length);
        for(File file:inputArray) {
            //路径
            //Path path=new Path("tmp\\firstCount\\"+file.toString().substring(6));
            Path path=new Path("hdfs:\\hadoop-master:9000\\user\\dell\\tmp\\firstCount"+file.toString().substring(6));
            //Path path=new Path("hdfs:\\localhost:9000\\user\\dell\\tmp\\firstCount"+file.toString().substring(6));
            //System.out.println(file.toString());
            WordCountJob.Job(file,path);
        }
        //对input所有文件进行WordCount，结果保存在output/allresult文件夹下
        WordCountAll.job();



    }
}
