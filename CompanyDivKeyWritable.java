package angad.file;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.WritableComparable;


public class CompanyDivKeyWritable implements WritableComparable{
	
	String stockSymbol;
	 int stockYear;
	 
	 

	 public void readFields(DataInput in) throws IOException {
	  stockSymbol = in.readUTF();
	  stockYear = in.readInt();
	 }

	
	 public void write(DataOutput out) throws IOException {
		 out.writeUTF(stockSymbol);
		 out.writeInt(stockYear);
	 }

	 
	 // stock year and symbol -sorting
	 
	 public int compareTo(Object object) {
		 CompanyDivKeyWritable other = (CompanyDivKeyWritable)object;
	  if (this.stockYear == other.stockYear){
	   return this.stockSymbol.compareTo(other.stockSymbol);
	  } else {
	   return (this.stockYear > other.stockYear) ? 1 : -1;
	  }
	 }
	 
	 @Override
	 public int hashCode() {
	  final int code = 29;
	  int total = 1;
	  total = code * total
	    + ((stockSymbol == null) ? 0 : stockSymbol.hashCode());
	  total = code * total + stockYear;
	  return total;
	 }

	
	 public boolean equals(Object object) {
	  if (this == object)
	   return true;
	  if (object == null)
	   return false;
	  if (getClass() != object.getClass())
	   return false;
	  CompanyDivKeyWritable other = ( CompanyDivKeyWritable ) object;
	  if (stockSymbol == null) {
	   if (other.stockSymbol != null)
	    return false;
	  } else if (!stockSymbol.equals(other.stockSymbol))
	   return false;
	  if (stockYear != other.stockYear)
	   return false;
	  return true;
	 }
	 
	 @Override
	 public String toString() {
			 return stockYear + ":"
	    + "\t" + stockSymbol;
		 
	 }

	 public String getStock_symbol() {
	  return stockSymbol;
	 }

	 public int getStock_year() {
	  return stockYear;
	 }

	 public void setStock_symbol(String stockSymbol) {
	  this.stockSymbol = stockSymbol;
	 }

	 public void setStock_year(int stockYear) {
	  this.stockYear = stockYear;
	 }



}

