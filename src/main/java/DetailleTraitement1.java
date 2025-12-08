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

public class DetailleTraitement1 {
	private static final String INPUT_PATH = "input-detaille/";
	private static final String OUTPUT_PATH = "output/detailleTraitement1-";
	private static final Logger LOG = Logger.getLogger(DetailleTraitement1.class.getName());

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
			// Une méthode pour créer des messages de log
			//LOG.info("MESSAGE INFO");

			line=line.replaceAll(" ", "");

			String[] words = line.split(";");
			// La ligne est vide : on s'arrête
			if (Arrays.equals(words, emptyWords) || words[0].equals("FACT_ID") || words[0].equals("CLIENT_ID"))
				return;

			Text customerKey;
			Text mapValue;


			if(filename.contains("FACT_TRANSACTIONS")){
				customerKey = new Text(words[2]); //client id
				mapValue  = new Text("F:"+words[10]); // F pour identifier rapidement fact dans le reducer, montant_total
			}
			else{
				customerKey = new Text(words[0]); //client id
				mapValue  = new Text("C:"+words[10]); // F pour identifier rapidement client dans le reducer, type client
			}
			context.write(customerKey,mapValue);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> typeClients = new ArrayList<>();
			ArrayList<String> montant_totals = new ArrayList<>();

			for (Text val : values){
				String[] vals = val.toString().split(":");
				if(vals[0].equals("C")){
					typeClients.add(vals[1]);
				}
				else if(vals[0].equals("F")){
					montant_totals.add(vals[1]);
				}
			}

			for(String typeClient : typeClients){
				for(String montant_total : montant_totals){
					context.write(new Text(typeClient), new Text(montant_total));
				}
			}
		}
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		private final static String[] emptyWords = { "" };

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			// Une méthode pour créer des messages de log
			//LOG.info("MESSAGE INFO");

			String[] words = line.split("\t");
			// La ligne est vide : on s'arrête
			if (Arrays.equals(words, emptyWords) || words.length < 2)
				return;

			Text customerKey = new Text(words[0]);
			Text mapValue = new Text(words[1]);

			context.write(customerKey, mapValue);
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double volume_totals = 0;
			int nb_transac = 0;

			for (Text val : values){
				volume_totals += Double.parseDouble(val.toString().replace(",", ".")); //probleme dans le csv avec , au lieu de .
				nb_transac ++;
			}

			context.write(key, new Text(nb_transac + " " + volume_totals));
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

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH+"FACT_TRANSACTIONS.csv")); //on selectionne que ce qu'il faut
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH+"DIM_CLIENTS.csv"));
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