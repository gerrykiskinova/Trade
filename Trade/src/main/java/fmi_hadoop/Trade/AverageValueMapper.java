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

public class AverageValueMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,DoubleWritable>{

String startPeriodT,endPeriodT,UNITT,categoryT;

@Override
public void configure(JobConf job) {
	startPeriodT = job.get("startPeriod", "");
	endPeriodT = job.get("endPeriod", "");
	//UNITT = job.get("UNIT", "");
	categoryT = job.get("category", "");
}

@Override
public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
		throws IOException {
	
	double startPeriod = 1971.06;
	double endPeriod = 2024.06;
	String category = "BEEF AND VEAL";
	double dataValue;
	
	if(!startPeriodT.isEmpty()) {
		startPeriod = Double.parseDouble(startPeriodT);
	}
	if(!endPeriodT.isEmpty()) {
		endPeriod = Double.parseDouble(endPeriodT);
	}
	if(!categoryT.isEmpty()) {
		category = categoryT;
	}
	
	String[] fields = value.toString().split(";");
	
	try {
        dataValue = Double.parseDouble(fields[2]);
    } catch (NumberFormatException e) {
        System.err.println("Invalid data value: " + fields[2]);
        return;
    }
	
	try {
		if(Double.parseDouble(fields[1])>=startPeriod&&
				Double.parseDouble(fields[1])<=endPeriod&&
				fields[9].equalsIgnoreCase(category)
				)
		{
		output.collect(new Text(fields[1] + " " + fields[1] + " " + fields[2] + " " + fields[3]),
				new DoubleWritable(Double.parseDouble(fields[2])));
		}
	}catch(NumberFormatException ex)
	{
		System.err.println(value.toString() + " - " + fields[0]);
		System.err.println(ex.getMessage());
	}
	
	try {
        
    } catch (NumberFormatException e) {
        System.err.println("Invalid data value: " + fields[2]);
        return;
    }
}

}
