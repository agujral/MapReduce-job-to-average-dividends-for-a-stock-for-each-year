package angad.file;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapred.Reporter;



public class CompanyDivMapper extends MapReduceBase implements
Mapper<LongWritable, Text, CompanyDivKeyWritable, CompanyDivWritable> {

	//counter for STATS
	public static enum STATS { R_2010, R_2009, R_2008, R_2007, R_2006, R_2005, R_2004, R_2003, R_2002, R_2001, R_2000, R_1999, R_1998, R_1997, R_1996, R_1995, R_1994, R_1993, R_1992, R_1991, R_1990, R_1989, R_1988, R_1987, R_1986, R_1985, R_1984, R_1983 };
	
	CompanyDivKeyWritable map_OKey = new CompanyDivKeyWritable();
	CompanyDivWritable map_OValue = new CompanyDivWritable();
	
	
	  //@Override
	 public void map(LongWritable map_Ikey,Text map_Ivalue,OutputCollector<CompanyDivKeyWritable, CompanyDivWritable> collector,
	    Reporter reporter) throws IOException 
	 {
	   String[] maparray = map_Ivalue.toString().split(",");
	   DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	   if (maparray.length == 4 & maparray[0].trim().equals("exchange") == false) 
	   {
		   
	    //Setting map output key 
	    map_OKey.setStock_symbol(maparray[1]);
	    try {
	     map_OKey.setStock_year(df.parse(maparray[2])
	       .getYear() + 1900);
	    } catch (ParseException i) {
	     System.out.println("Not a valid date : "
	       + maparray[2]);
	     i.printStackTrace();
	    }
	    
	    // map output value 
	   
	    map_OValue.setStock_exchange(maparray[0]);
	    map_OValue.setStock_symbol(maparray[1]);
	    map_OValue.setStock_date(maparray[2]);
	    map_OValue.setStock_dividend(Double
	      .valueOf(maparray[3]));
	    
	 // incrementing the counter   
	    
	switch (map_OKey.getStock_year())
{
    case 2010: reporter.getCounter(STATS.R_2010).increment(1);
		       break;
    case 2009: reporter.getCounter(STATS.R_2009).increment(1);
		       break;
    case 2008: reporter.getCounter(STATS.R_2008).increment(1);
               break; 
    case 2007: reporter.getCounter(STATS.R_2007).increment(1);
               break;
	case 2006: reporter.getCounter(STATS.R_2006).increment(1);
               break;
    case 2005: reporter.getCounter(STATS.R_2005).increment(1);
               break;
     case 2004: reporter.getCounter(STATS.R_2004).increment(1);
               break;
     case 2003: reporter.getCounter(STATS.R_2003).increment(1);
                break; 
     case 2002: reporter.getCounter(STATS.R_2002).increment(1);
                break;
     case 2001: reporter.getCounter(STATS.R_2001).increment(1);
                break;
     case 2000: reporter.getCounter(STATS.R_2000).increment(1);
                break;
     case 1999: reporter.getCounter(STATS.R_1999).increment(1);
                break;
     case 1998: reporter.getCounter(STATS.R_1998).increment(1);
                break;
     case 1997: reporter.getCounter(STATS.R_1997).increment(1);
                break;
     case 1996: reporter.getCounter(STATS.R_1996).increment(1);
                break;
     case 1995: reporter.getCounter(STATS.R_1995).increment(1);
                break;
     case 1994: reporter.getCounter(STATS.R_1994).increment(1);
                break;
     case 1993: reporter.getCounter(STATS.R_1993).increment(1);
                break;
     case 1992: reporter.getCounter(STATS.R_1992).increment(1);
                break;
     case 1991: reporter.getCounter(STATS.R_1991).increment(1);
                break;
     case 1990: reporter.getCounter(STATS.R_1990).increment(1);
                break;
     case 1989: reporter.getCounter(STATS.R_1989).increment(1);
                break;
     case 1988: reporter.getCounter(STATS.R_1988).increment(1);
                break;
     case 1987: reporter.getCounter(STATS.R_1987).increment(1);
                break;
     case 1986: reporter.getCounter(STATS.R_1986).increment(1);
                break;
     case 1985: reporter.getCounter(STATS.R_1985).increment(1);
                break;
     case 1984: reporter.getCounter(STATS.R_1984).increment(1);
                break;
     case 1983: reporter.getCounter(STATS.R_1983).increment(1);
                break;
     
    }
	    
	   collector.collect(map_OKey, map_OValue);
	    
	    
	  }
	
	}
}

