import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinTables {
	public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String input = "c," + value.toString();
			String[] tuple = input.split(",");// Tuple = ['c',Customer, ID, First Name, Last Name]
			context.write(new Text(tuple[1]), new Text(input));
		}
	}

	public static class TransactionMapper extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String input = "t," + value.toString();
			String[] tuple = input.split(",");// Tuple = ['t', Customer ID, Transaction ID, Date, Amount]
			context.write(new Text(tuple[1]), new Text(input));
		}
	}

	public static class JoinTablesReducer extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String[]> customers = new ArrayList<>();
			ArrayList<String[]> transactions = new ArrayList<>();
			for (Text val : values) {
				String input = val.toString();
				String[] tuple = input.split(",");
				if (tuple[0].equals("c"))
					customers.add(tuple);
				else
					transactions.add(tuple);
			}
			for (String[] customer : customers)
				for (String[] transaction : transactions) {
					String tuple = concate(customer, transaction);
					context.write(new Text(""), new Text(tuple));
				}

			
		}
	}

	private static String concate(String[] customer, String[] transaction) {
		String tuple = "[";
		for (int i = 1; i < customer.length; i++)
			tuple += customer[i] + ", ";
		for (int i = 2; i < transaction.length; i++)
			tuple += transaction[i]
					+ ((i == transaction.length - 1) ? "" : ", ");
		tuple += "]";
		return tuple;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(JoinTables.class);
		job.setReducerClass(JoinTablesReducer.class);
		org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job,
				new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
		org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job,
				new Path(args[1]), TextInputFormat.class,
				TransactionMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
