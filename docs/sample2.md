## Pipeline run description:
- _for customers - without SCD_
- _both input files from beginning_
- _where clause for silver customers_

_Input files:_

  - customers(csv)

  cus_0001.csv (1000 rows)
  
  cus_0002.csv (1003 rows) - 3 updates

  - orders (jsonl)
  
  ord_0001.jsonl (2000 records)
  
   ord_0002.jsonl (11records) - new

  - products (csv)
    
  prod_0001.csv (500 rows)
  
  prod_0002.csv (503 rows) - 3 updates

  **Bronze quarantine tables**
  
    orders_quarantine2 - 171 rows (quarantine condition - customerid, orderid, _rescued_data, order amount, order date, order status)
		
	products_quarantine2 - 68 rows (quarantine condition -  productid, rescued data, price, stock quantity)

  **Bronze tables**
  
    customers_bronze2 - 2003 (drop condition - customid, _rescued_data)
    
  	orders_bronze2 - 1957 (drop condition  - customid, orderid, _rescued_data)
  	
  	products_bronze2 - 1003 (drop condition - productid, rescued data) 

  **Silver tables**
  
    customers_silver2 - 969 (where clause - age > 69)

  	orders_silver2 - 1957
  
  	products_silver2 - 503
