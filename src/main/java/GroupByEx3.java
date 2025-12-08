
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GroupByEx3 {
    private static final String INPUT_PATH = "input-groupBy/";
    private static final String OUTPUT_PATH = "output/groupByEx3-";
    private static final Logger LOG = Logger.getLogger(GroupByEx3.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

        try {
            FileHandler fh = new FileHandler("out.log");
            fh.setFormatter(new SimpleFormatter());
            LOG.addHandler(fh);
        } catch (SecurityException | IOException e) {
            System.exit(1);
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            // sauter les titres
            if(key.get()==0)return;

            // séparation par virgule de chaque mot
            String[] cols = line.split(",");
            if (cols.length<=4) return;

            /* ======================== EXERCICE 3.1 et 3.2 ========================
            String orderDate= cols[2];
            String state=cols[10];
            String category=cols[14];

            String keyDateState = "DateState|"+orderDate+"|"+state;
            String keyDateCategory = "DateCategory|"+orderDate+"|"+category;

            // extraire (Sales)
            String salesStr = cols[cols.length-4];
            double sales = Double.parseDouble(salesStr);

            // GroupBy (OrderDate, State)
            context.write(new Text(keyDateState), new DoubleWritable(sales));

            // GroupBy (OrderDate, Category)
            context.write(new Text(keyDateCategory), new DoubleWritable(sales));
             ======================================================================== */

            /* ============================ EXERCICE 3.3  ============================ */
            String orderID= cols[1];

            // (Quantite)
            String quantityStr = cols[cols.length-1-2];
            double quantity = Double.parseDouble(quantityStr);

            // GroupBy (Order ID)
            context.write(new Text(orderID), new DoubleWritable(quantity));
            /* ======================================================================== */


        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int pdtDistinct=0;
            double sum=0.0;
            for (DoubleWritable v : values) {
                pdtDistinct++;
                sum += v.get();
            };
            context.write(key, new Text(pdtDistinct+"\t"+sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "GroupBy");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}