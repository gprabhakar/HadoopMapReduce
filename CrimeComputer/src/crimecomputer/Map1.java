/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package crimecomputer;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author prabh_000
 */
public class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
         private Text CrimeDetails=new Text();
         String Easting="";
         String Northing="";
         String CrimeType="";
         String words[]= new String[9];
         String crimedetails;

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
        {
    
        String line = value.toString(); 
        
        if(line!=null)
        {
          words=line.split(",");
                        
             if(words[1].charAt(0)=='2')
             {  
                 
                  int counter=0;
                  boolean flag;
                  boolean location=false;
                 for(int i=0;i<words.length;i++)
                  {
                   try
                   {   
                        Integer.parseInt(words[i]);
                        flag=true;
                        location=true;
                    }
                   catch(NumberFormatException e)
                   {
                        flag=false;
                        continue;
                    }
                   if(flag==true)
                   {
                        counter++;
                        if(counter==1)
                        {
                           Easting=words[i];
                           }
                        if(counter==2)
                        {
                           Northing=words[i];
                           try{
                           CrimeType=words[i+2];
                           }
                           catch(ArrayIndexOutOfBoundsException e)
                           {
                              CrimeType="Other crime"; 
                           }
                           break;
                         }
                    }
                  }
                 if(location==false)
                 {
                     System.out.println("\n~~~~~~~~~~~~~~~~~~Easting and Northing not found in the line!~~~~~~~~~~~~~~~~~~~\n");
                 }
                 if(Easting.matches("[0-9]+") && Northing.matches("[0-9]+"))
                 {
                 crimedetails=Easting.charAt(0)+"XXXX,"+ Northing.charAt(0)+"XXXX,"+CrimeType;
                 CrimeDetails.set(crimedetails);
                 output.collect(CrimeDetails, one);
                 }
              }
            }
          }           
        } 