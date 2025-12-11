import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class CrossBorderPipeline {

    private static final String INPUT_BD          = "input-bd/";
    private static final String STEP1_OUTPUT = "output/Step1ClientEnrich-";
    private static final String FINAL_OUTPUT = "output/CrossBorderStats-";
    private static final String MERCHANT_INPUT    = "input-bd/dimMarchand.csv";


    /* JOB 1 */
    public static class Step1Map extends Mapper<LongWritable, Text, Text, Text> {
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

            String[] cols = line.split(",");
            if (cols.length == 0) return;

            String firstCol = cols[0].replace("\"", "").trim().toLowerCase();
            if ("clientkey".equals(firstCol)) {
                return;
            }

            if (fichierActuel.contains("dimclient")) {
                // dimClient.csv:
                if (cols.length < 5) return;

                String clientKey     = cols[0].replace("\"", "").trim();
                String paysResidence = cols[4].replace("\"", "").trim();

                // C = Client
                String outVal = "C|" + paysResidence;
                context.write(new Text(clientKey), new Text(outVal));

            } else if (fichierActuel.contains("faittransactions")) {
                if (cols.length < 11) return;

                String clientKey       = cols[0].replace("\"", "").trim();
                String marchandKey     = cols[6].replace("\"", "").trim();
                String montantCompense = cols[8].replace("\"", "").trim();
                String scoreRisque     = cols[10].replace("\"", "").trim();

                // F = Fait
                String outVal = "F|" + marchandKey + "|" + montantCompense + "|" + scoreRisque;
                context.write(new Text(clientKey), new Text(outVal));
            }
        }
    }

    public static class Step1Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String paysClient = null;
            List<String> buf = new ArrayList<>();
            for (Text v : values) buf.add(v.toString());

            // trouver pays client
            for (String s : buf) {
                String[] parts = s.split("\\|");
                if (parts[0].equals("C")) {
                    paysClient = parts[1];
                }
            }
            if (paysClient == null) return;


            for (String s : buf) {
                String[] parts = s.split("\\|");
                if (parts[0].equals("F")) {
                    if (parts.length < 4) continue;
                    String marchandKey = parts[1];
                    String montantStr  = parts[2];
                    String scoreStr    = parts[3];

                    String outKey = marchandKey;
                    String outVal = "E|" + paysClient + "|" + montantStr + "|" + scoreStr;
                    context.write(new Text(outKey), new Text(outVal));
                }
            }
        }
    }


    /* JOB 2 */
    public static class Step2Map extends Mapper<LongWritable, Text, Text, Text> {
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

            // dimMarchand.csv
            if (fichierActuel.contains("dimmarchand")) {
                String[] cols = line.split(",");
                if (cols.length == 0) return;

                String firstCol = cols[0].replace("\"", "").trim().toLowerCase();
                if ("marchandkey".equals(firstCol)) {
                    return;
                }

                if (cols.length < 6) return;

                String marchandKey  = cols[0].replace("\"", "").trim();
                String paysMarchand = cols[5].replace("\"", "").trim();

                String outVal = "M|" + paysMarchand;
                context.write(new Text(marchandKey), new Text(outVal));

            } else {
                String[] kv = line.split("\t");
                if (kv.length < 2) return;

                String marchandKey = kv[0].trim();
                String payload     = kv[1].trim();

                context.write(new Text(marchandKey), new Text(payload));
            }
        }
    }

    public static class Step2Reduce extends Reducer<Text, Text, Text, Text> {
        private static class Agg {
            int n = 0;
            double sumMontant = 0.0;
            double sumScore   = 0.0;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String paysMarchand = null;
            Map<String, Agg> aggByClientCountry = new HashMap<>();

            List<String> buf = new ArrayList<>();
            for (Text v : values) buf.add(v.toString());

            // pays marchand
            for (String s : buf) {
                String[] parts = s.split("\\|");
                if (parts[0].equals("M")) {
                    paysMarchand = parts[1];
                }
            }
            if (paysMarchand == null) return;

            // transactions avec les infos supplémentaires
            for (String s : buf) {
                String[] parts = s.split("\\|");
                if (parts[0].equals("E")) {
                    if (parts.length < 4) continue;

                    String paysClient = parts[1];
                    String montantStr = parts[2].replace("\"", "").trim();
                    String scoreStr   = parts[3].replace("\"", "").trim();
                    if (montantStr.isEmpty() || scoreStr.isEmpty()) continue;

                    try {
                        double montant = Double.parseDouble(montantStr);
                        double score   = Double.parseDouble(scoreStr);

                        Agg a = aggByClientCountry.get(paysClient);
                        if (a == null) {
                            a = new Agg();
                            aggByClientCountry.put(paysClient, a);
                        }
                        a.n++;
                        a.sumMontant += montant;
                        a.sumScore   += score;

                    } catch (NumberFormatException ignored) {
                    }
                }
            }

            for (Map.Entry<String, Agg> e : aggByClientCountry.entrySet()) {
                String paysClient = e.getKey();
                Agg a = e.getValue();
                if (a.n == 0) continue;
                double avgScore = a.sumScore / a.n;

                String outKey = paysClient + "|" + paysMarchand+ "|" +"M"+ key.toString();;
                String outVal = a.n + "|" + a.sumMontant + "|" + avgScore;

                context.write(new Text(outKey), new Text(outVal));
            }
        }
    }


    /* On lance Job 1 ensuite Job 2*/
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // ---- Job 1: jointure des transactions avec pay client ----
        String step1OutDir = STEP1_OUTPUT + Instant.now().getEpochSecond();

        Job job1 = new Job(conf, "Step1ClientEnrich");
        job1.setJarByClass(CrossBorderPipeline.class);

        job1.setMapperClass(Step1Map.class);
        job1.setReducerClass(Step1Reduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(INPUT_BD));
        FileOutputFormat.setOutputPath(job1, new Path(step1OutDir));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // ---- Job 2: jointure avec marchands et aggregation par (paysClient, paysMarchand) ----
        Job job2 = new Job(conf, "CrossBorderStats");
        job2.setJarByClass(CrossBorderPipeline.class);

        job2.setMapperClass(Step2Map.class);
        job2.setReducerClass(Step2Reduce.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        // dimMarchand + Step1 sortie en tant qu'entrée
        FileInputFormat.addInputPath(job2, new Path(MERCHANT_INPUT));
        FileInputFormat.addInputPath(job2, new Path(step1OutDir));

        String finalOutDir = FINAL_OUTPUT + Instant.now().getEpochSecond();
        FileOutputFormat.setOutputPath(job2, new Path(finalOutDir));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
