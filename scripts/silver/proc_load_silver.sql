/*
=====================================================================================
	This stored procedure performs the ETL (Extract,Transform,Load) process to 
	populate the 'Silver' schema tables from the 'Bronze' schema.

	Actions Performed:
	-Truncates Silver tables.
	- Insert transformed and cleaned data from Bronze into Silver tables.
======================================================================================
*/
CREATE OR REPLACE PROCEDURE silver.load_silver_layers()
LANGUAGE plpgsql
AS $$
BEGIN

	TRUNCATE TABLE silver.crm_cust_info;
	
	  RAISE NOTICE '===========================================';
	  RAISE NOTICE 'Inserting Data Info: silver.crm_cust_info';
	  RAISE NOTICE '===========================================';
	
	
	INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
	
	SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END cst_marital_status,
	
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' 
		ELSE 'n/a'
	END cst_gndr,
	cst_create_date
	FROM 
	(
	SELECT *, rank()
	OVER(
		PARTITION BY cst_id
		ORDER BY cst_create_date DESC
	) as flag_last
	FROM bronze.crm_cust_info
	WHERE  cst_id IS NOT NULL
	)t 
	WHERE flag_last = 1;
	
	TRUNCATE TABLE silver.crm_prd_info;

	  RAISE NOTICE '===========================================';
	  RAISE NOTICE 'Inserting Data Info: silver.crm_prd_info';
	  RAISE NOTICE '===========================================';

	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') as cat_id,
	SUBSTRING(prd_key FROM 7 FOR LENGTH(prd_key)) as prd_key,
	prd_nm,
	COALESCE(prd_cost,0) AS prd_cost,
	
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mounthing'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END as prd_line,
	CAST(prd_start_dt AS DATE),
	CAST(LEAD(prd_start_dt -1) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE) AS prd_end_dt_test
	FROM bronze.crm_prd_info;
	
	TRUNCATE TABLE silver.crm_sales_details;

	  RAISE NOTICE '===========================================';
	  RAISE NOTICE 'Inserting Data Info: silver.crm_sales_details';
	  RAISE NOTICE '===========================================';

	
	INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_oredr_dt,
		sls_ship_date,
		sls_due_date,
		sls_sales,
		sls_quantity,
		sls_price
	)
	
	SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS TEXT)) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
		
	CASE 
		WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS TEXT)) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	
	CASE 
		WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS TEXT)) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	
	CASE WHEN sls_price IS NULL OR sls_price<=0
		THEN sls_sales/NULLIF(sls_quantity,0)
		ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details;
	
	TRUNCATE TABLE silver.erp_cust_az12;

	  RAISE NOTICE '===========================================';
	  RAISE NOTICE 'Inserting Data Info: silver.erp_cust_az12';
	  RAISE NOTICE '===========================================';

	
	INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
	)
	
	SELECT
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
		ELSE cid
	END AS cid,
	
	CASE WHEN bdate > NOW() THEN NULL
		ELSE bdate
	END AS bdate,
	
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
	
	FROM bronze.erp_cust_az12;
	
	
	TRUNCATE TABLE silver.erp_loc_a101;

	  RAISE NOTICE '===========================================';
	  RAISE NOTICE 'Inserting Data Info: silver.erp_loc_a101';
	  RAISE NOTICE '===========================================';

	
	INSERT INTO silver.erp_loc_a101(
	city,
	cntry
	)
	SELECT
	REPLACE(city, '-', '') AS city,
	
	CASE WHEN TRIM(cntry) = 'DE' THEN 'GERMANY'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United State'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
				
	FROM bronze.erp_loc_a101;
	
	
	TRUNCATE TABLE silver.erp_px_cat_g1v2;

	  RAISE NOTICE '===========================================';
	  RAISE NOTICE 'Inserting Data Info: silver.erp_px_cat_g1v2';
	  RAISE NOTICE '===========================================';

	
	INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
	)
	SELECT
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
END;
$$;
