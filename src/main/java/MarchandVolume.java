import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class MarchandVolume {

    private static final String INPUT_PATH  = "input-detaille/";
    private static final String OUTPUT_PATH = "output/MarchandVolume-";

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private String fichierActuel;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fichierActuel = fileSplit.getPath().getName().toLowerCase();
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] cols = line.split(";");
            if (cols.length == 0) return;

            String firstCol = cols[0].trim().toLowerCase();

            // DIM_MARCHANDS.csv
            if (fichierActuel.contains("dim_marchands")) {

                if ("marchand_id".equals(firstCol)) return;
                if (cols.length < 2) return;

                String marchandId   = cols[0].trim();
                String nomMarchand  = cols[1].trim();

                // M = Marchand
                String outVal = "M|" + nomMarchand;
                context.write(new Text(marchandId), new Text(outVal));

                // FACT_TRANSACTIONS.csv
            } else if (fichierActuel.contains("fact_transactions")) {

                if ("fact_id".equals(firstCol)) return;
                if (cols.length < 11) return;

                String marchandId   = cols[6].trim();
                String montantStr   = cols[10].trim();

                montantStr = montantStr.replace(",", ".");

                // F = Fact
                String outVal = "F|" + montantStr;
                context.write(new Text(marchandId), new Text(outVal));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String nomMarchand = null;
            double sumMontant  = 0.0;
            int nbTx           = 0;

            List<String> buf = new ArrayList<>();
            for (Text v : values) buf.add(v.toString());

            // extraire nom marchand
            for (String s : buf) {
                String[] parts = s.split("\\|");
                if (parts[0].equals("M")) {
                    if (parts.length >= 2) {
                        nomMarchand = parts[1];
                    }
                }
            }
            if (nomMarchand == null) return;

            // aggrégations
            for (String s : buf) {
                String[] parts = s.split("\\|");
                if (parts[0].equals("F")) {
                    if (parts.length < 2) continue;
                    String montantStr = parts[1].trim();
                    if (montantStr.isEmpty()) continue;

                    try {
                        double montant = Double.parseDouble(montantStr);
                        sumMontant += montant;
                        nbTx++;
                    } catch (NumberFormatException ignored) {
                    }
                }
            }

            if (nbTx > 0) {
                String outKey = nomMarchand;
                String outVal = nbTx + "|" + sumMontant;
                context.write(new Text(outKey), new Text(outVal));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "MarchandVolume");
        job.setJarByClass(MarchandVolume.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(
                job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond())
        );

        job.waitForCompletion(true);
    }
}
