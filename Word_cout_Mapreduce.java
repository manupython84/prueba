import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class QuotationAnalyzer {

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text wordToCount = new Text();
        private final static IntWritable one = new IntWritable(1);

        // Palabra objetivo que queremos contar
        private String targetWord;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Obtenemos la palabra a buscar del contexto
            Configuration conf = context.getConfiguration();
            targetWord = conf.get("targetWord").toLowerCase();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Convertimos la línea a minúsculas y la tokenizamos
            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase());

            while (itr.hasMoreTokens()) {
                String currentWord = itr.nextToken();

                // Si la palabra actual coincide con la palabra objetivo, emitimos (palabra, 1)
                if (currentWord.equals(targetWord)) {
                    wordToCount.set(currentWord);
                    context.write(wordToCount, one);
                }
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            // Sumamos todas las ocurrencias de la palabra
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: QuotationAnalyzer <input_path> <output_path> <word_to_count>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("targetWord", args[2]); // Pasamos la palabra objetivo como parámetro

        Job job = Job.getInstance(conf, "Word Count for Specific Word");
        job.setJarByClass(QuotationAnalyzer.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}