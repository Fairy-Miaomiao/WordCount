package test;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMap extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text word = new Text();
    private Set<String> stopWordList = new HashSet<String>();
    private Set<String> puncList = new HashSet<String>();

    protected void setup(Context context){
        // 停词文件路径
        //路径
        //Path stopWordFile = new Path("F:\\FBDP\\作业\\作业5\\stop-word-list.txt");
        //Path stopWordFile = new Path("hdfs:\\localhost:9000\\user\\dell\\materials\\stop-word-list.txt");
        Path stopWordFile = new Path("hdfs:\\hadoop-master:9000\\user\\dell\\materials\\stop-word-list.txt");
        // 标点符号文件路径
        //Path puncFile = new Path("F:\\FBDP\\作业\\作业5\\punctuation.txt");
        Path puncFile = new Path("hdfs:\\hadoop-master:9000\\user\\dell\\materials\\punctuation.txt");
        //Path puncFile = new Path("hdfs:\\localhost:9000\\user\\dell\\materials\\punctuation.txt");
        readWordFile(stopWordFile);
        readPuncFile(puncFile);
    }

    private void readWordFile(Path stopWordFile) {
        try {
            BufferedReader fis1 = new BufferedReader(new FileReader(stopWordFile.toString()));
            String stopWord = null;
            while ((stopWord = fis1.readLine()) != null) {
                stopWordList.add(stopWord);
            }
        } catch (IOException ioe) {
            System.err.println("Exception while reading stop word file '"
                    + stopWordFile + "' : " + ioe.toString());
        }
    }
    private void readPuncFile(Path puncFile) {
        try {
            BufferedReader fis = new BufferedReader(new FileReader(puncFile.toString()));
            String punc;
            //System.out.println(stopWordFile.toString());
            while ((punc = fis.readLine()) != null) {
                puncList.add(punc);
            }
        } catch (IOException ioe) {
            System.err.println("Exception while reading punctuation file '"
                    + puncFile + "' : " + ioe.toString());
        }
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            token = token.toLowerCase();
            //
            Pattern pattern = Pattern.compile("[\\d]");
            Matcher matcher = pattern.matcher(token);
            token = matcher.replaceAll("").trim();
            if (token.length() >= 3 && !stopWordList.contains(token)) {
                for(String puncWord: puncList){
                    token = token.replace(puncWord.substring(1), "");
                }
                if(token.length() >= 3 && !stopWordList.contains(token)){
                    word.set(token);
                    context.write(word, new LongWritable(1));
                }
            }
        }
    }
}
