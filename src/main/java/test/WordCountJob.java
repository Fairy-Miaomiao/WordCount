package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class WordCountJob {


    public static void Job(File inputFile,Path path) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        //System.out.println("1");
        Job job = new Job(conf, "Count");
        job.setJarByClass(WordCountJob.class);
        job.setMapperClass(WordCountMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //List<String> other_args = new ArrayList<String>();

        FileInputFormat.setInputPaths(job, new Path(inputFile.toString()));
        FileOutputFormat.setOutputPath(job, path);
        job.waitForCompletion(true);

        conf = new Configuration();
        //路径
        // String final_every = "E:\\IdeaProjects\\Hadoop05\\output\\top100result\\"+inputFile.toString().substring(6);
        String final_every="hdfs:\\hadoop-master:9000\\user\\dell\\input\\shakespeare\\"+inputFile.toString().substring(6);
        //String final_every="hdfs:\\localhost:9000\\user\\dell\\input\\shakespeare\\"+inputFile.toString().substring(6);
        //前K个词要输出到哪个目录
        conf.set("final", final_every);
        //System.out.println("2");
        Job sortjob = new Job(conf, "Sort");
        sortjob.setJarByClass(WordCountSort.class);
        sortjob.setMapperClass(WordCountSort.Map.class);
        sortjob.setReducerClass(WordCountSort.Reduce.class);

        // 设置Map输出类型
        sortjob.setMapOutputKeyClass(IntWritable.class);
        sortjob.setMapOutputValueClass(Text.class);

        // 设置Reduce输出类型
        sortjob.setOutputKeyClass(Text.class);
        sortjob.setOutputValueClass(IntWritable.class);

        //设置MultipleOutputs的输出格式
        //这里利用MultipleOutputs进行对文件输出
        MultipleOutputs.addNamedOutput(sortjob, "topKMOS", TextOutputFormat.class, Text.class, Text.class);

        //System.out.println("3");
        // 设置输入和输出目录
        FileInputFormat.addInputPath(sortjob, path);
        //System.out.println("4");
        //路径
        //FileOutputFormat.setOutputPath(sortjob, new Path("tmp\\sort\\" + inputFile.toString().substring(6)));
        FileOutputFormat.setOutputPath(sortjob, new Path("hdfs:\\hadoop-master:9000\\user\\dell\\tmp\\sort\\"+inputFile.toString().substring(6)));
        //FileOutputFormat.setOutputPath(sortjob, new Path("hdfs:\\localhost:9000\\user\\dell\\tmp\\sort\\"+inputFile.toString().substring(6)));
        //System.out.println("tmp\\sort\\" + inputFile.toString().substring(6));
        sortjob.waitForCompletion(true);

    }
}

