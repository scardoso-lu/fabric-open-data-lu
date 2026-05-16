CREATE TABLE [dbo].[customers] (

	[customer_id] int NULL, 
	[first_name] varchar(8000) NULL, 
	[last_name] varchar(8000) NULL, 
	[first_order] date NULL, 
	[most_recent_order] date NULL, 
	[number_of_orders] int NULL, 
	[customer_lifetime_value] decimal(38,6) NULL
);