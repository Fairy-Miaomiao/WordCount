package test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class WordCountSort {
    public static class Map extends Mapper<Object, Text, IntWritable, Text> {

        // 输出key 词频
        IntWritable outKey = new IntWritable();
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

            context.write(outKey, outValue);
        }

    }

    /**
     * 根据词频排序
     *
     * @author zx
     */
    public static class Reduce extends Reducer<IntWritable, Text, Text, IntWritable> {

        private static MultipleOutputs<Text, IntWritable> mos = null;

        //要获得前K个频率最高的词
        private static final int k = 100;

        //用TreeMap存储可以利用它的排序功能
        //这里用 WCString 因为TreeMap是对key排序，且不能唯一，而词频可能相同，要以词频为Key就必需对它封装
        private static TreeMap<WordCountRev, String> tm = new TreeMap<WordCountRev, String>(new Comparator<WordCountRev>() {
            /**
             * 默认是从小到大的顺序排的，现在修改为从大到小
             * @param o1
             * @param o2
             * @return
             */
            public int compare(WordCountRev o1, WordCountRev o2) {
                return o2.compareTo(o1);
            }

        });

        /*
         * 以词频为Key是要用到reduce的排序功能
         */
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(text, key);
                tm.put(new WordCountRev(key.get(), text.toString()), text.toString());

                //TreeMap以对内部数据进行了排序，最后一个必定是最小的
                if (tm.size() > k) {
                    tm.remove(tm.lastKey());
                }

            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            String path = context.getConfiguration().get("final");
            mos = new MultipleOutputs<Text, IntWritable>(context);
            Set<java.util.Map.Entry<WordCountRev, String>> set = tm.entrySet();
            int count = 1;
            for (java.util.Map.Entry<WordCountRev, String> entry : set) {
//                mos.write("topKMOS", new Text(entry.getValue()), new IntWritable(entry.getKey().getValue()), path);
                mos.write("topKMOS", new Text(String.valueOf(count) + ": " + entry.getValue() + ", " + String.valueOf(entry.getKey().getKey())), NullWritable.get(), path);
                count += 1;
            }
            tm.clear();
            mos.close();
        }
    }
}
