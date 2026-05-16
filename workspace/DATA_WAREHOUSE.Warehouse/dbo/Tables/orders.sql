CREATE TABLE [dbo].[orders] (

	[order_id] int NULL, 
	[customer_id] int NULL, 
	[order_date] date NULL, 
	[status] varchar(8000) NULL, 
	[credit_card_amount] decimal(38,6) NULL, 
	[coupon_amount] decimal(38,6) NULL, 
	[bank_transfer_amount] decimal(38,6) NULL, 
	[gift_card_amount] decimal(38,6) NULL, 
	[amount] decimal(38,6) NULL
);