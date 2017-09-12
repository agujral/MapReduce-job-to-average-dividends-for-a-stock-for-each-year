package angad.file;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;



public class CompanyDivReducer extends MapReduceBase implements
Reducer<CompanyDivKeyWritable, CompanyDivWritable, Text, Text> {
	
	
	  Map<String, String> symbol_Map = new HashMap<String, String>();
	  
	  Text reduce_OKey = new Text();
	  Text reduce_OValue = new Text();
	  
	  //enum- enriched
	  public static enum ENRICHED {enriched, not_enriched};
	  	  
	  public void val_to_map(JobConf jobconf) throws IOException 
	  {
		 
	        Path companiesFilePath = new Path("/symbol_description.csv");
			FileSystem fs = FileSystem.get(jobconf);
			FSDataInputStream iStream = fs.open(companiesFilePath);
			InputStreamReader iStreamReader = new InputStreamReader(iStream);
			BufferedReader br = new BufferedReader(iStreamReader);
			String line = "";
			while ((line = br.readLine()) != null) 
			{
					String line1 = line.replaceAll("\"", "");
					String line2 = line1.replaceAll(",", "\t");
				    String[] symbol_array = line2.split("\t");
				    if (symbol_array.length == 2) 
				    {
				     symbol_Map.put(symbol_array[0].trim(), symbol_array[1]);
				    }
		    } 
	  }
	  
	  
	  public void configure(JobConf jobconf) {
		   try {
		    val_to_map(jobconf);
		   } catch (IOException i) {
		    System.out.println(" Error:");
		    i.printStackTrace();
		   }
		  }

	
	 public void reduce(CompanyDivKeyWritable reduce_IKey,Iterator<CompanyDivWritable> reduce_IValues,
			    OutputCollector<Text, Text> collector, Reporter reporter)
	            throws IOException 
	 {
	        
		   double Total_Dividends = 0;
		   CompanyDivWritable reduce_IValue = new CompanyDivWritable();
		   StringBuilder str = new StringBuilder("");
		   int value = 0;
		   reduce_OKey.set(reduce_IKey.toString());
		   while (reduce_IValues.hasNext()) 
		   {
		   reduce_IValue = reduce_IValues.next();
		   // calculating the total dividend
		   Total_Dividends = Total_Dividends + reduce_IValue.getStock_dividend();    
		   value++;
		   }
		   
		   
		    str.append("(");
		    str.append(symbol_Map.get(reduce_IKey.getStock_symbol().trim()));
		    
		    str.append(")");
		    
		    // counter for enriched records
		    
		    if(symbol_Map.get(reduce_IKey.getStock_symbol().trim()) == null )
		    {
		    reporter.getCounter(ENRICHED.not_enriched).increment(1);
		    }
		    else
		    {
		    reporter.getCounter(ENRICHED.enriched).increment(1);	
		    }
		    str.append("\t");
		    str.append(Total_Dividends / value);
		    reduce_OValue.set(str.toString());
		    collector.collect(reduce_OKey, reduce_OValue);
	    }
	 
	 

}
