## Popeline run description:

_Input files:_

  - customers(csv)
    
  cus_0001.csv (1000 rows)

  - orders (jsonl)
    
  ord_0001.jsonl (2000 records)

  - products (csv)
    
  prod_0001.csv (500 rows)

  _Quarantine tables for all 3 inputs, SCD 2 for customers and products tables._

**Bronze quarantine tables**

	customers_quarantine - 95 rows (quarantine condition - customerid, _rescued_data, age, email)
		
	orders_quarantine - 167 rows (quarantine condition - customerid, orderid, _rescued_data, order amount, order date, order status)
		
  products_quarantine - 34 rows (quarantine condition -  productid, rescued data, price, stock quantity)

**Bronze tables**

	customers_bronze - 1000 (drop condition - customid, _rescued_data)
  
	orders_bronze - 1949  (drop condition  - customid, orderid, _rescued_data)
  
  products_bronze - 500 (drop condition - productid, rescued data) 

**Silver tables**

customers_bronze - 1000

orders_bronze - 1949 

products_bronze - 500

************************************************************************************************
## Updates on inputs

**Bronze quarantine tables**

Products - 3 values updated

Orders - new file with 11 new rows

Customers -  3 values updated

**Bronze tables**

customers_bronze - 2003

orders_bronze - 1957 

products_bronze -1003

**Silver tables**

customers_bronze - 1003

orders_bronze - 1957

products_bronze - > 503


