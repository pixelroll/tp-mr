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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Join {
	private static final String INPUT_PATH = "input-join/";
	private static final String OUTPUT_PATH = "output/join-";
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
		private final static String[] emptyWords = { "" };
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			// Une méthode pour créer des messages de log
			//LOG.info("MESSAGE INFO");

			String[] words = line.split("\\|"); // le | est un caractere special en regex, il faut l'echapper
			// La ligne est vide : on s'arrête
			if (Arrays.equals(words, emptyWords))
				return;

			Text customerKey;
			Text mapValue;

			if(words[1].split("#")[0].equals("Customer")){
				customerKey = new Text(words[0]);
				mapValue  = new Text("N:"+words[1]); // N pour identifier rapidement un nom dans le reducer
			}
			else{
				customerKey = new Text(words[1]);
				mapValue  = new Text("C:"+words[8]); // C pour identifier rapidement un commentaire dans le reducer
			}
			context.write(customerKey,mapValue);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<Text> customerNames = new ArrayList<>();
			ArrayList<Text> comments = new ArrayList<>();

			for (Text val : values){
				String[] vals = val.toString().split(":");
				if(vals[0].equals("N")){
					customerNames.add(new Text(vals[1]));
				}
				else if(vals[0].equals("C")){
					comments.add(new Text(vals[1]));
				}
			}

			for(Text customerName : customerNames){
				for(Text comment : comments){
					context.write(customerName, comment);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Join");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class); //output text car commentaire

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}