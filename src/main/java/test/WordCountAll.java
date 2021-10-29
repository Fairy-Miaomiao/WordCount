package test;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.fusesource.leveldbjni.All;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class WordCountAll {

    public static class AllMap extends Mapper<Object, Text, Text, LongWritable> {

        LongWritable outKey = new LongWritable();
        Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer st = new StringTokenizer(value.toString());
            while (st.hasMoreTokens()) {
                String element = st.nextToken();
                if (Pattern.matches("\\d+", element)) {
                    outKey.set(Integer.parseInt(element));
                } else {
                    outValue.set(element);
                }
            }
            context.write(outValue, outKey);
        }

    }

    @SuppressWarnings("deprecation")
    public static void job() throws IOException, ClassNotFoundException, InterruptedException{

        Configuration conf = new Configuration();

        Job job = new Job(conf, "Combiner");
        job.setJarByClass(WordCountAll.class);
        job.setMapperClass(AllMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> other_args = new ArrayList<String>();
        //路径
        //FileInputFormat.setInputPaths(job, new Path("tmp\\firstcount\\*\\"));
        FileInputFormat.setInputPaths(job, new Path("hdfs:\\hadoop-master:9000\\user\\dell\\tmp\\firstCount\\*\\"));
        //FileInputFormat.setInputPaths(job, new Path("hdfs:\\localhost:9000\\user\\dell\\tmp\\firstCount\\*\\"));
        //FileOutputFormat.setOutputPath(job, new Path("tmp\\allcount"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs:\\hadoop-master:9000\\user\\dell\\tmp\\allcount"));
        //FileOutputFormat.setOutputPath(job, new Path("hdfs:\\localhost:9000\\user\\dell\\tmp\\allcount"));
        job.waitForCompletion(true);

        conf = new Configuration();
        //路径
        //String topKout = "E:\\IdeaProjects\\Hadoop05\\output\\allresult\\all";
        String topKout = "hdfs:\\hadoop-master:9000\\user\\dell\\output\\allresult\\all";
        //String topKout = "hdfs:\\localhost:9000\\user\\dell\\output\\allresult\\all";
        //System.out.println(topKout);
        //前K个词要输出到哪个目录
        conf.set("final", topKout);
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
        //路径
        //FileInputFormat.addInputPath(sortjob, new Path("tmp\\allcount"));
        FileInputFormat.addInputPath(sortjob, new Path("hdfs:\\hadoop-master:9000\\user\\dell\\tmp\\allcount"));
        //FileInputFormat.addInputPath(sortjob, new Path("hdfs:\\localhost:9000\\user\\dell\\tmp\\allcount"));
        System.out.println("4");
        //FileOutputFormat.setOutputPath(sortjob, new Path("tmp\\sort\\all"));
        FileOutputFormat.setOutputPath(sortjob, new Path("tmphdfs:\\hadoop-master:9000\\user\\dell\\sort\\all"));
        //FileOutputFormat.setOutputPath(sortjob, new Path("tmphdfs:\\localhost:9000\\user\\dell\\sort\\all"));
        //System.out.println("tmp\\sort\\" + inputFile.toString().substring(6));
        sortjob.waitForCompletion(true);
    }
}

