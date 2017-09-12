package angad.file;

import angad.file.CompanyDivMapper;
import angad.file.CompanyDivReducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapred.*;




public class DivAvgByYearJob 
{
	    
	public static void main( String[] args )  throws IOException
	   
    {	
		 // configuration
		 Configuration conf = new Configuration();
		  JobConf jobconf = new JobConf(conf);

		  jobconf.setJobName(" Company Average dividend");

		  jobconf.setJarByClass(DivAvgByYearJob.class);
		  
		  jobconf.setReducerClass(CompanyDivReducer.class);

		  // key-value pair
		  jobconf.setMapOutputKeyClass(CompanyDivKeyWritable.class);
		  jobconf.setMapOutputValueClass(CompanyDivWritable.class);

		  jobconf.setOutputKeyClass(Text.class);
		  jobconf.setOutputValueClass(Text.class);
		  
		  // globbing
		  Path glob = new Path(args[0]);


		  FileSystem fs = FileSystem.get(new Configuration());
		  FileStatus [] files = fs.globStatus(glob);

         
		 for (FileStatus file : files )
		 {
		      String name = file.getPath().getName();
		      MultipleInputs.addInputPath(jobconf, new Path(args[1] + name),TextInputFormat.class, CompanyDivMapper.class);
		  }
		   
		 
		  FileOutputFormat.setOutputPath(jobconf, new Path(args[2]));
	      jobconf.setOutputFormat(TextOutputFormat.class);

	      
		  JobClient.runJob(jobconf);
		 

	 } 

} 


        

