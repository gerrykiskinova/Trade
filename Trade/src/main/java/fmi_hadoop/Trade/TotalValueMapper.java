package fmi_hadoop.Trade;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TotalValueMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,DoubleWritable> {
	private double startPeriod ;
    private double endPeriod ;
	private static final Map<String, Integer> monthToNumber = new HashMap<>();
    
    // Статичен блок за инициализация на месеците и техните числови стойности
    static {
        monthToNumber.put("January", 1);
        monthToNumber.put("February", 2);
        monthToNumber.put("March", 3);
        monthToNumber.put("April", 4);
        monthToNumber.put("May", 5);
        monthToNumber.put("June", 6);
        monthToNumber.put("July", 7);
        monthToNumber.put("August", 8);
        monthToNumber.put("September", 9);
        monthToNumber.put("October", 10);
        monthToNumber.put("November", 11);
        monthToNumber.put("December", 12);
    }
	
	
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) 
    		throws IOException {
        String line = value.toString();
        String[] fields = line.split(",");
        
       
        if (!isHeader(line) && isValid(fields)) {
            String periodString = fields[1];
            //double period = parsePeriodToDouble(periodString);
            double period = Double.parseDouble(fields[1].trim());
            double dataValue = Double.parseDouble(fields[2].trim());
            //context.write(new Text(period), new DoubleWritable(dataValue));
            
            if (period >= startPeriod && period <= endPeriod) {
                output.collect(new Text(String.valueOf(period)), new DoubleWritable(dataValue));
            }
            
        }
    }
    @Override
	public void configure(JobConf job) {
    	  // Задаване на periodStart и periodEnd от конфигурацията
    	startPeriod = Double.parseDouble(job.get("startPeriod", ""));
    	endPeriod = Double.parseDouble(job.get("endPeriod", ""));
	}
    private boolean isHeader(String line) {
        return line.contains("Series_reference") || line.contains("Period") || line.contains("Data_value")
        		|| line.contains("STATUS") || line.contains("UNITS") || line.contains("MAGNTUDE") 
        		|| line.contains("Subject") || line.contains("Group") || line.contains("Series_title_1") 
        		|| line.contains("Series_title_2") || line.contains("Series_title_3")
        		|| line.contains("Series_title_4") || line.contains("Series_title_5");
    }

    private boolean isValid(String[] fields) {
        return fields.length >= 8; 
    }
    
    /*private double parsePeriodToDouble(String periodString) {
        try {
            String[] parts = periodString.split("\\.");
            int year = Integer.parseInt(parts[0]);
            int month = monthToNumber.getOrDefault(parts[1], 0); // Вземаме числото на месеца

            if (month == 0) {
                throw new IllegalArgumentException("Невалиден месец: " + parts[1]);
            }

            // Пример за комбиниране в double формат: година.месяц
            return year + month / 100.0;
        } catch (Exception e) {
            System.err.println("Грешка при парсване на периода: " + periodString + " - " + e.getMessage());
            return 0.0; // Връщаме 0.0 при грешка в преобразуването
        }
    }*/
}
