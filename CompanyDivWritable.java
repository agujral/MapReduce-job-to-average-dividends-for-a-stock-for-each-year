package angad.file;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.Writable;

public class CompanyDivWritable implements Writable {
	
	 String stockExchange;
	 String stockSymbol;
	 String stockDate;
	 double stockDividend;

	 @Override
	 public void readFields(DataInput in) throws IOException {
	  stockExchange = in.readUTF();
	  stockSymbol = in.readUTF();
	  stockDate = in.readUTF();
	  stockDividend = in.readDouble();
	 }
	
	 @Override
	 public void write(DataOutput out) throws IOException {
	  out.writeUTF(stockExchange);
	  out.writeUTF(stockSymbol);
	  out.writeUTF(stockDate);
	  out.writeDouble(stockDividend);
	 }


	
	 public String getStock_exchange() {
		  return stockExchange;
		 }

		 public String getStock_symbol() {
		  return stockSymbol;
		  
		 }

		 public String getStock_date() {
		  return stockDate;
		 }

		 public double getStock_dividend() {
			  return stockDividend;
			 }

			 public void setStock_exchange(String stockExchange) {
			  this.stockExchange = stockExchange;
			 }

			 public void setStock_symbol(String stockSymbol) {
			  this.stockSymbol = stockSymbol;
			 }

			 public void setStock_date(String stockDate) {
			  this.stockDate = stockDate;
			 }
			 
			 public void setStock_dividend(double stockDividend) {
				  this.stockDividend = stockDividend;
				 }

}

