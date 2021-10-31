import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;




public class WordCount {
    public static class TokenizerFileMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        static enum CountersEnum { INPUT_WORDS }
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> tingci= new HashSet<String>();
        private Set<String> biaodianfuhao = new HashSet<String>();
        private Configuration conf;
        private BufferedReader fis; // 保存文件输入流

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles(); //

                Path patternsPath = new Path(patternsURIs[0].getPath());
                String patternsFileName = patternsPath.getName().toString();
                parseSkipFile(patternsFileName);

                Path biaodianfuhaoPath = new Path(patternsURIs[1].getPath());
                String biaodianfuhaoFileName = biaodianfuhaoPath.getName().toString();
                parseSkipbiaodianfuhao(biaodianfuhaoFileName);
            }
        }


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        static enum CountersEnum { INPUT_WORDS }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private Set<String> tingci = new HashSet<String>();
        private Set<String> biaodianfuhao = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;


        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();


            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();

                Path patternsPath = new Path(patternsURIs[0].getPath());
                String patternsFileName = patternsPath.getName().toString();
                parseSkipFile(patternsFileName);

                Path biaodianfuhaoPath = new Path(patternsURIs[1].getPath());
                String biaodianfuhaoFileName = biaodianfuhaoPath.getName().toString();
                parseSkipbiaodianfuhao(biaodianfuhaoFileName);
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    tingci.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file "
                        + StringUtils.stringifyException(ioe));
            }
        }

        private void parseSkipbiaodianfuhao(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    biaodianfuhao.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file "
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            for (String pattern : biaodianfuhao) {
                line = line.replaceAll(pattern, " ");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String one_word = itr.nextToken();


                if(one_word.length()<3) {
                    continue;
                }

                if(Pattern.compile("^[-\\+]?[\\d]*$").matcher(one_word).matches()) {
                    continue;
                }

                if(tingci.contains(one_word)){
                    continue;
                }

                word.set(one_word);
                context.write(word, one);

                Counter counter = context.getCounter(
                        CountersEnum.class.getName(),
                        CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

  public static class SortReducer extends Reducer <SortKey,IntWritable,Text,IntWritable> {
 
    private MultipleOutputs<Text,IntWritable> mos;
    private IntWritable valueInfo = new IntWritable();
    private Text keyInfo = new Text();
    private HashMap<String,Integer> map=new HashMap<>();
    protected void setup(Context context) throws IOException,InterruptedException{
         mos = new MultipleOutputs<Text,IntWritable>(context);
 }
    protected void cleanup(Context context) throws IOException,InterruptedException{
        mos.close();
 }
    @Override
    public void reduce(SortKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
     String docName=key.y.split("#")[1];
    int rank=map.getOrDefault(docName,1);
     if(rank<=100){
    keyInfo.set(Integer.toString(rank)+":"+key.y.split("#")[0]+", ");
    valueInfo.set(key.x);
     rank+=1;
    map.put(docName,rank);
    mos.write(docName,keyInfo,valueInfo); 
     } 
    }
 }
    public static class SortAllReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
        private Text result = new Text();
        int rank=1;

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val: values){
                if(rank > 100)
                {
                    break;
                }
                result.set(val.toString());
                String str=rank+": "+result+", "+key;
                rank++;
                context.write(new Text(str),NullWritable.get());
            }
        }
    }

