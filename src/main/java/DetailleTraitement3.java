import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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

public class DetailleTraitement3 {
	private static final String INPUT_PATH = "input-detaille/";
	private static final String OUTPUT_PATH = "output/detailleTraitement3-";
	private static final Logger LOG = Logger.getLogger(DetailleTraitement3.class.getName());

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
		private final static String[] emptyWords = { "" };
		private String filename;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException { //permet la futur identification de la source de la ligne lu
			// Récupérer le nom du fichier en cours de traitement
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			filename = fileSplit.getPath().getName();
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			line=line.replaceAll(" ", "");

			String[] words = line.split(";");
			// La ligne est vide ou header : on s'arrête
			if (Arrays.equals(words, emptyWords) || words[0].equals("FACT_ID") || words[0].equals("PLAN_ID"))
				return;

			Text planKey;
			Text mapValue;

			if(filename.contains("FACT_TRANSACTIONS")){
				planKey = new Text(words[9]); // PLAN_ID est à l'index 9
				mapValue = new Text("F:" + words[10]); // MONTANT_TOTAL est à l'index 10
			}
			else{
				planKey = new Text(words[0]); // PLAN_ID est à l'index 0 dans DIM_PLANS
				mapValue = new Text("D:" + words[1] + ":" + words[10]); // NOM_PLAN à index 1, NIVEAU_PLAN à index 10
			}
			context.write(planKey, mapValue);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> nomPlans = new ArrayList<>();
			ArrayList<String> niveauPlans = new ArrayList<>();
			ArrayList<String> montants = new ArrayList<>();

			for (Text val : values){
				String[] vals = val.toString().split(":");
				if(vals[0].equals("D")){
					nomPlans.add(vals[1]);
					niveauPlans.add(vals[2]);
				}
				else if(vals[0].equals("F")){
					montants.add(vals[1]);
				}
			}

			for(int i = 0; i < nomPlans.size(); i++){
				String nomPlan = nomPlans.get(i);
				String niveauPlan = niveauPlans.get(i);
				for(String montant : montants){
					context.write(new Text(nomPlan + ";" + niveauPlan + ";1;" + montant), new Text(""));
				}
			}
		}
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		private final static String[] emptyWords = { "" };

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			String[] words = line.split("\t");
			// La ligne est vide : on s'arrête
			if (Arrays.equals(words, emptyWords) || words.length < 1)
				return;

			String[] planInfo = words[0].split(";");
			if(planInfo.length < 4)
				return;

			String nomPlan = planInfo[0];
			String niveauPlan = planInfo[1];
			String compteur = planInfo[2];
			String montant = planInfo[3];

			Text agrKey = new Text(nomPlan + "|" + niveauPlan);
			Text agrValue = new Text(compteur + ";" + montant);

			context.write(agrKey, agrValue);
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double volume_totals = 0;
			int nb_transac = 0;

			for (Text val : values){
				String[] parts = val.toString().split(";");
				if(parts.length >= 2){
					nb_transac += Integer.parseInt(parts[0]);
					volume_totals += Double.parseDouble(parts[1].replace(",", "."));
				}
			}

			double montant_moyen = nb_transac > 0 ? volume_totals / nb_transac : 0;
			context.write(key, new Text(nb_transac + " " + volume_totals + " " + montant_moyen));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String timestamp = String.valueOf(Instant.now().getEpochSecond());

		Job job = new Job(conf, "Join");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class); //output text car commentaire

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH+"FACT_TRANSACTIONS.csv"));
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH+"DIM_PLANS.csv"));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + timestamp + "-join"));
		
		if(job.waitForCompletion(true)){
			Job job2 = new Job(conf, "agregation");

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);

			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH + timestamp + "-join")); //on recupere la sortie du join
			FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH + timestamp + "-aggregation"));

			job2.waitForCompletion(true);
		}
	}
}