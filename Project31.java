import java.io.IOException; 
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

public class Project31
{
	public static class Mapper1 extends Mapper<LongWritable,Text,Text,Text>
	{
		@Override
		public void map(LongWritable key, Text value,Context c) throws IOException,InterruptedException
		{
			double tem,dewp,windsp;

			String str = value.toString();

			String[] strlist = str.split("(,? *,?)( +)|,");			//String[] str1 = s1.split("(,? *,?)( +)|,");

			String[] hr = strlist[2].split("_");
			
			String stnid = strlist[0];

			String m = hr[0].substring(4,6);

			String sec = "",s = "";

			int hr = Integer.parseInt(hr[1]);

			temp = Double.parseDouble(strlist[3]);
			dewp = Double.parseDouble(strlist[4]);
			windsp = Double.parseDouble(strlist[12]);
	
			if(temp != 9999.9 && dewp != 9999.9 && windsp != 999.9 && strlist[0].charAt(0) != S)
			{

				s = ""+temp+" "+dewp+" "+windsp;				
			
				if(hr<6)
				{
					sec = "1";
				}
	
				else if(hr>5 || hr<12)
				{
					sec = "2";
				}
	
				else if(hr>11 || hr<18)
				{
					sec = "3";
				}
		
				else
				{
					sec = "4";
				}
			
				c.write(new Text(""+stnid+""+m+""+sec),new Text(s));
			}
		}
	}

	 public static class Reducer1 extends Reducer<Text,ArrayWritable,Text,Text>
	 {
		  @Override 
		  public void reduce(Text key, Iterable<Text>values, Context c) throws IOException,InterruptedException
		  {
			double temp,dewp,windsp,count,outercount;
			String s;
			
			for (Text val: values)
			{
				outercount++;
				String[] strlist = val.split(" ");
				for (int i=0;i<3;i++)			   // iterate
				{
					if(count % 3 == 0)
					{
						temp = temp + Double.parseDouble(strlist[0]);                    
					}
					if(count % 3 == 1)
					{
						dewp = dewp + Double.parseDouble(strlist[1]);
					}
					if(count % 3 == 2)
					{
						windsp = windsp +  Double.parseDouble(strlist[2]);
					}
					count++;
				}
			}
			
			temp = temp/outercount;
			dewp = dewp/outercount;
			windsp = windsp/outercount;

			s = " " + temp + " " + dewp + " " + windsp;

			c.write(new Text(""+stnid+""+m+""+sec),new Text(s));
  		}
	}

	public static class Mapper2 extends Mapper<LongWritable,Text,Text,Text>
	{
		@Override
		public void map(LongWritable key, Text value,Context c) throws IOException,InterruptedException
		{
			String key = strlist[0].substring(0,8);
			
			c.write(new Text(key),value);			
						
		}
	}

	 public static class Reducer2 extends Reducer<Text,Text,Text,Text>
	 {
		  @Override 
		  public void reduce(Text key, Iterable<Text>values, Context c) throws IOException,InterruptedException
		  {
			String s="";
			
			for (ArrayWritable val: values)
			{
				for (int i=0;i<4;i++)
				{   	
					s = s + val.toString();
				}
			}

			c.write(key,new Text(s));
  		}
	}

		
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{ 
            Configuration conf = new Configuration(); 
            Job j1 = new Job(conf); 
     	    j1.setJobName("Project31 Job1"); 
     	    j1.setJarByClass(Project31.class); 

	    //Mapper input and output 

            j1.setMapOutputKeyClass(Text.class); 
            j1.setMapOutputValueClass(Text.class); 

           //Reducer input and output 
     	   j1.setOutputKeyClass(Text.class); 
           j1.setOutputValueClass(Text.class); 

          //file input and output of the whole program 
     	  j1.setInputFormatClass(TextInputFormat.class); 
          j1.setOutputFormatClass(TextOutputFormat.class); 
      
	  //Set the mapper class  
   	  j1.setMapperClass(Mapper1.class); 

	  //set the combiner class for custom combiner 
	  //j1.setCombinerClass(Reducer1.class); 

	 //Set the reducer class 
	 j1.setReducerClass(Reducer1.class); 

	 //set the number of reducer if it is zero means there is no reducer 
         //j1.setNumReduceTasks(0); 
            
	 FileOutputFormat.setOutputPath(j1, new Path(args[1])); 
         FileInputFormat.addInputPath(j1, new Path(args[0])); 
         j1.waitForCompletion(true); 

	Job j2 = new Job(conf); 
     	j2.setJobName("Project31 Job2"); 
     	J2.setJarByClass(Project31.class); 

	//Mapper input and output 

            j2.setMapOutputKeyClass(Text.class); 
            j2.setMapOutputValueClass(Text.class); 

           //Reducer input and output 
     	   j2.setOutputKeyClass(Text.class); 
           j2.setOutputValueClass(Text.class); 

          //file input and output of the whole program 
     	  j2.setInputFormatClass(TextInputFormat.class); 
          j2.setOutputFormatClass(TextOutputFormat.class); 
      
	  //Set the mapper class  
   	  j2.setMapperClass(Mapper2.class); 

	  //set the combiner class for custom combiner 
	  //j2.setCombinerClass(Reducer2.class); 

	 //Set the reducer class 
	 j2.setReducerClass(Reducer2.class); 

	 //set the number of reducer if it is zero means there is no reducer 
         //j2.setNumReduceTasks(0); 
            
	 FileOutputFormat.setOutputPath(j2, new Path(args[2])); 
         FileInputFormat.addInputPath(j2, new Path(args[1])); 
         j2.waitForCompletion(true); 
      } 
} 		 