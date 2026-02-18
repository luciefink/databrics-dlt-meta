## Pipeline run description:
- _both input files from first run_
- _dev/prod setting in onboarding4.json_
- _metadata for bronze tables - set in onboarding4.json_
- _dqe bronze jsons - expect (in pipeline log - see number of expec records but all are added to bronze) and expect_or_quarantinte_
  
_Input files:_

  - customers(csv)

  cus_0001.csv (1000 rows)
  
  cus_0002.csv (1003 rows) - 3 updates

  - orders (jsonl)
  
  ord_0001.jsonl (2000 records)
  
   ord_0002.jsonl (11records) - new

**Bronze tables**
  
    customers_bronze4 - 2003 (expec condition - customid, _rescued_data)
    
  	orders_bronze4 - 2011 (expect condition  - customid, orderid, _rescued_data)

**Bronze quarantine tables**

	customers_quarantine - 190 rows (quarantine condition - customerid, _rescued_data, age, email)
		
	orders_quarantine - 171 rows (quarantine condition - customerid, orderid, _rescued_data, order amount, order date, order status)

**Silver tables**

	customers_silver4 - 1003

	orders_silver4 - 1960 (expect or drop)

