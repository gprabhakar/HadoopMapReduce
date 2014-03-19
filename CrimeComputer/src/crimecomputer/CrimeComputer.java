/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package crimecomputer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
/**
 *
 * @author prabh_000
 */
public class CrimeComputer {

    public static void main(String[] args) throws IOException {
        // TODO code application logic here
        JobConf conf = new JobConf(CrimeComputer.class);
	      conf.setJobName("CrimeComputer");
	
	      conf.setOutputKeyClass(Text.class);
	      conf.setOutputValueClass(IntWritable.class);
              
              System.out.println("\n Please select a region defintion:\n 1.Highest one digit of Northing & Easting \n 2.Highest two digits of Northing & Easting \n 3.Highest three digits of Northing & Easting \n Enter: 1,2 or 3 \n ");
              BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	      String option=br.readLine();
              if(option.equals("1"))
              {
              conf.setMapperClass(Map1.class);  
              }
              else if(option.equals("2"))
              conf.setMapperClass(Map2.class);
              else if(option.equals("3"))
              {
              conf.setMapperClass(Map3.class);  
              }
              else
              {
              System.out.println("Invalid Input!");
              System.exit(0);
              }

	      
              System.out.println("\n Please select the number of Mappers: 1, 2 or 5? \n");
              option=br.readLine();
              if(option.equals("1"))
              conf.setNumMapTasks(1);
              if(option.equals("2"))
              conf.setNumMapTasks(2);
              if(option.equals("5"))
              conf.setNumMapTasks(5); 
              
              System.out.println("\n Please select the number of Reducers: 1, 2 or 5? \n");
              option=br.readLine();
              if(option.equals("1"))
              conf.setNumReduceTasks(1);
              if(option.equals("2"))
              conf.setNumReduceTasks(2);
              if(option.equals("5"))
              conf.setNumReduceTasks(5); 
              
              conf.setCombinerClass(Reduce.class);
	      conf.setReducerClass(Reduce.class);
	
              
	      conf.setInputFormat(TextInputFormat.class);
	      conf.setOutputFormat(TextOutputFormat.class);
	
	      FileInputFormat.setInputPaths(conf, new Path(args[0]));
	      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
              JobClient.runJob(conf);
        
    }
}

