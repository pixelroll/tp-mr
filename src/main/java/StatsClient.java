import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.List;

public class StatsClient {
    private static final String INPUT_PATH = "input-bd/";
    private static final String OUTPUT_PATH = "output/StatsClient-";
    private static final Logger LOG = Logger.getLogger(Join.class.getName());

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

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private String fichierActuel;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Récupérer le nom du fichier en cours de traitement
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fichierActuel = fileSplit.getPath().getName().toLowerCase();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            if (line.isEmpty()) return;
            if (line.toLowerCase().startsWith("clientkey")) return;

            if (fichierActuel.contains("dimclient") && line.toLowerCase().startsWith("clientkey")) {
                return;
            }
            if (fichierActuel.contains("faittransactions") && line.toLowerCase().startsWith("clientkey")) {
                return;
            }

            String[] cols = line.split(",");
            String firstCol = cols[0].replace("\"", "").trim().toLowerCase();
            if ("clientkey".equals(firstCol)) {
                return;
            }

            // dimClient.csv:
            if (fichierActuel.toLowerCase().contains("dimclient")) {
                if(cols.length<6)return;
                String clientKey = cols[0];
                String age = cols[1];
                String paysResidence = cols[4];
                String segmentClient = cols[5];

                // C = Client
                String outVal = "C|" + age + "|" + paysResidence + "|" + segmentClient;
                context.write(new Text(clientKey), new Text(outVal));

                // faitTransactions.csv:
            } else if (fichierActuel.toLowerCase().contains("faittransactions")) {
                if(cols.length<11)return;
                String clientKey = cols[0];
                String montantCompense = cols[8];
                String scoreRisque = cols[10];

                // F = Fait
                String outVal = "F|" + montantCompense + "|" + scoreRisque;
                context.write(new Text(clientKey), new Text(outVal));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String age = null;
            String paysResidence = null;
            String segmentClient = null;
            double sumMontant = 0.0;
            double sumScore = 0.0;
            int nbTx = 0;

            // on peut utiliser Iterable qu'une seule fois donc il faut un buffer
            List<String> buf = new ArrayList<>();
            for (Text v : values) buf.add(v.toString());


            // Extraire attributs client
            for (String s : buf) {
                String[] parts = s.split("\\|");
                if (parts[0].equals("C")) {
                    age = parts[1];
                    paysResidence = parts[2];
                    segmentClient = parts[3];
                }
            }

            // Agrégations des transactions
            for (String s : buf) {
                String[] parts = s.split("\\|");
                if (parts[0].equals("F")) {
                    double montant = Double.parseDouble(parts[1]);
                    double score = Double.parseDouble(parts[2]);
                    sumMontant += montant;
                    sumScore += score;
                    nbTx++;
                }
            }

            if (nbTx>0 && age!=null) {
                double avgScore = sumScore / nbTx;

                // key = ClientKey|age|pays|segment
                String outKey = key.toString()
                        + "|" + age
                        + "|" + paysResidence
                        + "|" + segmentClient;

                // value = nbTx|sumMontant|avgScore
                String outVal = nbTx + "|" + sumMontant + "|" + avgScore;

                context.write(new Text(outKey), new Text(outVal));
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "StatsClient");

        job.setJarByClass(StatsClient.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}