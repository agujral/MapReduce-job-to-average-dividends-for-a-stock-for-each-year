# MapReduce-job-to-average-dividends-for-a-stock-for-each-year
MapReduce job to average dividends for a stock for each year

The job produce a single file where each line documents average dividends for a stock for each year. In another words for a year – stock combination compute average dividends. 
The file was sorted by year and then by stock symbol
Format is <year>:<stock symbol & name>\t<dividends’ average>

The original input data contained stock symbols, but did not have the full name of the stock. 

The job was to enrich the result with the stock’s full name/description from symbol_description.csv. If symbol_description.csv does not contain stock symbol then the symbol itself was used. If the symbol was matched to its full name then the final result was the original symbol and the full name in parenthesis: <symbol>(<name>)
