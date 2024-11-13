package fmi_hadoop.Trade;

import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

/**
 * Hello world!
 */
public class App extends JFrame {
    public static void main(String[] args) {
    	 App form = new App();
    	 
    }
    public App() {
    	init();
    }
    private JTextField periodFieldStart, periodFieldEnd, categoryField;
	private JComboBox<String> unitsComboBox, processingOption;
	private JTextArea resultArea;
    private void init() {
    	 setTitle("Data Filter Application");
         setSize(600, 400);
         setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
         setLocationRelativeTo(null);
         setLayout(new BorderLayout());

    	JPanel filterPanel = new JPanel(new GridLayout(5, 2, 10, 10));
        filterPanel.setBorder(BorderFactory.createTitledBorder("Filters"));
        
        // Period Filter
        filterPanel.add(new JLabel("Period Start (YYYY.MM):"));
        periodFieldStart = new JTextField();
        filterPanel.add(periodFieldStart);

        filterPanel.add(new JLabel("Period End (YYYY.MM):"));
        periodFieldEnd = new JTextField();
        filterPanel.add(periodFieldEnd);

        // Units Filter
        filterPanel.add(new JLabel("Units:"));
        unitsComboBox = new JComboBox<>(new String[]{"", "Index", "Dollars"});
        filterPanel.add(unitsComboBox);

        // Category Filter
        filterPanel.add(new JLabel("Category:"));
        categoryField = new JTextField();
        filterPanel.add(categoryField);
        
        // Падащо меню за вид обработка
        filterPanel.add(new JLabel("Oportunity:"));
        processingOption = new JComboBox<>(new String[] {"Обща стойност", "Средна стойност"});
        filterPanel.add(processingOption);
        
        JPanel resultPanel = new JPanel(new BorderLayout());
        resultPanel.setBorder(BorderFactory.createTitledBorder("Results"));
        resultArea = new JTextArea();
        resultArea.setEditable(false);
        resultPanel.add(new JScrollPane(resultArea), BorderLayout.CENTER);

        // Button Panel
        JPanel buttonPanel = new JPanel();
        JButton searchButton = new JButton("Search");
        //searchButton.addActionListener(new SearchAction());
        buttonPanel.add(searchButton);

        // Add panels to JFrame
        add(filterPanel, BorderLayout.NORTH);
        add(buttonPanel, BorderLayout.CENTER);
        add(resultPanel, BorderLayout.SOUTH);
        setVisible(true);
        
        searchButton.addActionListener(new ActionListener (){
    		@Override
    		public void actionPerformed(ActionEvent e) {
    			String periodStart = periodFieldStart.getText();
                String periodEnd = periodFieldEnd.getText();
                String units = (String) unitsComboBox.getSelectedItem();
                String category = categoryField.getText();
                String selectedOption = (String) processingOption.getSelectedItem();
                
                startHadoop(periodStart, periodEnd, units, category, selectedOption);
    			StringBuilder resultText = new StringBuilder("Search Results:\n");
    			
    			// Example of handling empty filters (for actual data filtering logic, you would connect to a dataset)
                if (!periodStart.isEmpty() || !periodEnd.isEmpty()) {
                    resultText.append("Period: ").append(periodStart).append(" to ").append(periodEnd).append("\n");
                }
                if (units != null && !units.isEmpty()) {
                    resultText.append("Units: ").append(units).append("\n");
                }
                if (category != null && !category.isEmpty()) {
                    resultText.append("Category: ").append(category).append("\n");
                }

                // Show results in result area (in a real application, replace with actual data filtering and display)
                resultText.append("Filtered data would be displayed here...");
                resultArea.setText(resultText.toString());
    		}
        });
    }
        protected void startHadoop(String periodStart, String periodEnd, String units, String category, String selectedOption) {
    		Configuration conf = new Configuration();
             /*conf.set("periodStart", periodStart);
             conf.set("periodEnd", periodEnd);
             conf.set("units", units);
             conf.set("category", category);*/
            
            Path inputPath = new Path("hdfs://127.0.0.1:9000/input/6.csv");
            Path outputPath = new Path("hdfs://127.0.0.1:9000/result");
            
            JobConf job = new JobConf(conf, App.class);
            
            //критерия, по който искаме да търсим 
            
            job.set("periodStart", periodStart);
            job.set("periodEnd", periodEnd);
            job.set("units", units);
            job.set("category", category);
            
            //job.setMapperClass(MyMapper.class);
            //job.setReducerClass(SalesReducer.class);
            if (selectedOption.equals("Обща стойност")) {
                job.setMapperClass(TotalValueMapper.class);
                job.setReducerClass(TotalValueReducer.class);
            } else if (selectedOption.equals("Средна стойност")) {
                job.setMapperClass(AverageValueMapper.class);
                job.setReducerClass(AverageValueReducer.class);
            }
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            
            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            
            try {
            	FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);
            	if(fs.exists(outputPath)) {
            		fs.delete(outputPath, true);
            	}
            	RunningJob task = JobClient.runJob(job);
            	if(task.isSuccessful()) {
            		FileReader fr = new FileReader("hdfs://127.0.0.1:9000/result/part-00000");
            		BufferedReader br = new BufferedReader(fr);
            		System.out.println(br.readLine());
            	}
            	else {
            		System.out.println("Job went caput!");
            	}

            }
            catch(IOException e) {
            	System.out.println(e.toString());
            }
    	}
    }

