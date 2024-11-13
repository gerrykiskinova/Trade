package fmi_hadoop.Trade;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/*public class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
	 private String periodStart;
	 private String periodEnd;
	 private String units;
	 private String category;

	@Override
	/*public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
			String[] columns = value.toString().split(",");
			
			String period = columns[1];  // Assuming Period is the first field (e.g., "2024.01")
		    String unit = columns[4];    // Units (e.g., "Index")
		    String cat = columns[2];     // Category (e.g., "Beef and Veal")
		    int dataValue = Integer.parseInt(columns[3]);*/
			
		/*if(columns[7].toLowerCase().contains(filter.toLowerCase())) {
			Text outputKey = new Text(columns[7] + "-" + columns[3]);
			double price = Double.parseDouble(columns[2]);
			output.collect(outputKey, new DoubleWritable(price));*/
			
			/*boolean matchesFilters = true;
			// Filter by period (if specified)
	        if (!periodStart.isEmpty() && !period.equals(periodStart)) {
	            matchesFilters = false;
	        }
	        if (!periodEnd.isEmpty() && !period.equals(periodEnd)) {
	            matchesFilters = false;
	        }

	        // Filter by units (if specified)
	        if (units != null && !units.isEmpty()) {
	            matchesFilters = false;
	        }

	        // Filter by category (if specified)
	        if (category != null && !category.isEmpty()) {
	            matchesFilters = false;
	        }
	        // If all filters match, emit the key-value pair
	        if (matchesFilters) {
	            // Emit the key-value pair: Key could be the period or category, and value is the data value
	            // You can change the key to something else depending on the type of result you want
	            context.write(new Text(period), new IntWritable(dataValue));
	        }
		}
		
	}
	@Override
	public void configure(JobConf job) {
		filter = job.get("filter", "");
	}*/
