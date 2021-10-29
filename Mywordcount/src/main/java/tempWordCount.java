import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class tempWordCount {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString()," ,.\":\t\n");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().toLowerCase());
                context.write(word, one);
            }
        }
    }

/*InverseMapper类的内容参考
      public class InverseMapper<K, V> extends Mapper<K,V,V,K> {  


          // The inverse function.  Input keys and values are swapped.
          @Override  
          public void map(K key, V value, Context context  
                          ) throws IOException, InterruptedException {  
            context.write(value, key);  
          }
      }
*/

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        if ((remainingArgs.length != 2) && (remainingArgs.length != 5)) {
            System.err.println("Usage: wordcount <in> <out> [-skip biaodianfuhao skipPatternFile]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerFileMapper.class);
        job.setCombinerClass(WordCount.IntSumReducer.class);
        job.setReducerClass(WordCount.IntSumReducer.class);
        //job.setPartitionerClass(NewPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        Path tempDir = new Path("linshiresult" );
        FileOutputFormat.setOutputPath(job, tempDir);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if(job.waitForCompletion(true))
        {

            Job sortJob = Job.getInstance(conf, "sort file");
            sortJob.setJarByClass(WordCount.class);

            FileInputFormat.addInputPath(sortJob, tempDir);
            sortJob.setInputFormatClass(SequenceFileInputFormat.class);


            sortJob.setMapperClass(InverseMapper.class);
            sortJob.setReducerClass(WordCount.SortFileReducer.class);

            Path tempFileDir = new Path("results" );
            FileOutputFormat.setOutputPath(sortJob, tempFileDir);

            List<String> files = Arrays.asList("shakespearealls11", "shakespeareantony23", "shakespeareas12",
                    "shakespearecomedy7", "shakespearecoriolanus24", "shakespearecymbeline17", "shakespearefirst51",
                    "shakespearehamlet25", "shakespearejulius26", "shakespeareking45", "shakespearelife54",
                    "shakespearelife55", "shakespearelife56", "shakespearelovers62", "shakespeareloves8",
                    "shakespearemacbeth46", "shakespearemeasure13", "shakespearemerchant5", "shakespearemerry15",
                    "shakespearemidsummer16", "shakespearemuch3", "shakespeareothello47", "shakespearepericles21",
                    "shakespearerape61", "shakespeareromeo48", "shakespearesecond52", "shakespearesonnets59",
                    "shakespearesonnets", "shakespearetaming2", "shakespearetempest4", "shakespearethird53",
                    "shakespearetimon49", "shakespearetitus50", "shakespearetragedy57", "shakespearetragedy58",
                    "shakespearetroilus22", "shakespearetwelfth20", "shakespearetwo18", "shakespearevenus60",
                    "shakespearewinters19");

            for (String fileName : files) {
                MultipleOutputs.addNamedOutput(sortJob, fileName, TextOutputFormat.class,Text.class, NullWritable.class);
            }

            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);




            if(sortJob.waitForCompletion(true)){
                Job job1 = Job.getInstance(conf, "all word count");
                job1.setJarByClass(WordCount.class);
                job1.setMapperClass(WordCount.TokenizerMapper.class);
                job1.setCombinerClass(WordCount.IntSumReducer.class);
                job1.setReducerClass(WordCount.IntSumReducer.class);
                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(IntWritable.class);

                for (int i = 0; i < remainingArgs.length; ++i) {
                    if ("-skip".equals(remainingArgs[i])) {
                        job1.addCacheFile(new Path(remainingArgs[++i]).toUri());
                        job1.addCacheFile(new Path(remainingArgs[++i]).toUri());
                        job1.getConfiguration().setBoolean("wordcount.skip.patterns", true);
                    } else {
                        otherArgs.add(remainingArgs[i]);
                    }
                }

                FileInputFormat.addInputPath(job1, new Path(otherArgs.get(0)));
                Path tempAllDir = new Path("linshioutput" );
                FileOutputFormat.setOutputPath(job1, tempAllDir);
                job1.setOutputFormatClass(SequenceFileOutputFormat.class);

                if(job1.waitForCompletion(true)){
                    Job sortJob1 = Job.getInstance(conf, "sort all");
                    sortJob1.setJarByClass(WordCount.class);

                    FileInputFormat.addInputPath(sortJob1, tempAllDir);

                    sortJob1.setInputFormatClass(SequenceFileInputFormat.class);
                    sortJob1.setMapperClass(InverseMapper.class);
                    sortJob1.setReducerClass(WordCount.SortAllReducer.class);

                    FileOutputFormat.setOutputPath(sortJob1, new Path(otherArgs.get(1)));

                    sortJob1.setOutputKeyClass(IntWritable.class);
                    sortJob1.setOutputValueClass(Text.class);


                    System.exit(sortJob1.waitForCompletion(true) ? 0 : 1);
                }
            }
        }
    }
}