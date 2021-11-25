copy call_center from '<tpcds_root_path>call_center/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy catalog_page from '<tpcds_root_path>catalog_page/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy catalog_returns from '<tpcds_root_path>catalog_returns/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy catalog_sales from '<tpcds_root_path>catalog_sales/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy customer_address from '<tpcds_root_path>customer_address/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy customer_demographics from '<tpcds_root_path>customer_demographics/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy customer from '<tpcds_root_path>customer/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy date_dim from '<tpcds_root_path>date_dim/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy household_demographics from '<tpcds_root_path>household_demographics/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy income_band from '<tpcds_root_path>income_band/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy inventory from '<tpcds_root_path>inventory/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy item from '<tpcds_root_path>item/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy promotion from '<tpcds_root_path>promotion/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy reason from '<tpcds_root_path>reason/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy ship_mode from '<tpcds_root_path>ship_mode/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy store from '<tpcds_root_path>store/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy store_returns from '<tpcds_root_path>store_returns/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy store_sales from '<tpcds_root_path>store_sales/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy time_dim from '<tpcds_root_path>time_dim/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy warehouse from '<tpcds_root_path>warehouse/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy web_page from '<tpcds_root_path>web_page/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy web_returns from '<tpcds_root_path>web_returns/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy web_sales from '<tpcds_root_path>web_sales/' iam_role '<rs_role_arn>' gzip delimiter '|';
copy web_site from '<tpcds_root_path>web_site/' iam_role '<rs_role_arn>' gzip delimiter '|';
 
select count(*) from call_center;  -- 48
select count(*) from catalog_page;  -- 36000
select count(*) from catalog_returns;  -- 432018033
select count(*) from catalog_sales;  -- 4320078880
select count(*) from customer;  -- 30000000
select count(*) from customer_address;  -- 15000000
select count(*) from customer_demographics;  -- 1920800
select count(*) from date_dim;  -- 73049
select count(*) from household_demographics;  -- 7200
select count(*) from income_band;  -- 20
select count(*) from inventory;  -- 1033560000
select count(*) from item;  -- 360000
select count(*) from promotion;  -- 1800
select count(*) from reason;  -- 67
select count(*) from ship_mode;  -- 20
select count(*) from store;  -- 1350
select count(*) from store_returns;  -- 863989652
select count(*) from store_sales;  -- 8639936081
select count(*) from time_dim;  -- 86400
select count(*) from warehouse;  -- 22
select count(*) from web_page;  -- 3600
select count(*) from web_returns;  -- 216003761
select count(*) from web_sales;  -- 2159968881
select count(*) from web_site;  -- 66
