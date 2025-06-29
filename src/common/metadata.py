from pyspark.sql.types import *
from common.config import Config

class MetadataFactory:

    @staticmethod
    def get_metadata_instance(config, layer: str):
        layer = layer.lower()
        if layer == "bronze":
            return BronzeMetadata(config)
        elif layer == "silver":
            return SilverMetadata(config)
        elif layer == "gold":
            return GoldMetadata(config)
        else:
            raise ValueError(f"Invalid metadata layer: {layer}")

class BronzeMetadata:

  def __init__(self, config):
    self.config = config
  
  def getDateDimensionSchema(self):
    
    date_dim_schema = StructType([
        StructField("date", DateType(), True),
        StructField("year", IntegerType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("month_name", StringType(), True),
        StructField("week_of_year", IntegerType(), True),
        StructField("day_of_month", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("day_name", StringType(), True),
        StructField("is_weekend", BooleanType(), True),
        StructField("is_month_start", BooleanType(), True),
        StructField("is_month_end", BooleanType(), True)
    ])
    return date_dim_schema
  
  def getDataSelectQuery(self, table_name):
    if table_name == "invoices":
      query = f'''
      SELECT * FROM c 
      WHERE STARTSWITH(c.bill_datetime, "{self.config.yesterdays_date}")
      ''' 
      return query
    
    elif table_name == "expenses":
      query = f'''
      SELECT * FROM c 
      WHERE c.expense_date = "{self.config.yesterdays_date}"
      ''' 
      return query
    
    elif table_name == "stores":
      query = """
          SELECT * FROM Stores
      """
      return query
    
    elif table_name == "products":
      query = """
          SELECT * FROM Products
      """
      return query
    
    else:
      raise ValueError(f"Invalid Table Name: {table_name}")
      return None

  def getDatabaseCreateQuery(self):
    return f"CREATE DATABASE IF NOT EXISTS `{self.config.catalog}`.`{self.config.bronze_db}`"

  def getTableCreateQuery(self, table_name):
    if table_name == "invoices":
      query_create_bronze_table_invoices = f"""
        CREATE TABLE IF NOT EXISTS `{self.config.catalog}`.`{self.config.bronze_db}`.invoices (
          invoice_id      STRING,
          store_id        STRING,
          bill_datetime   TIMESTAMP,
          cashier_id      STRING,
          payment_method  STRING,
          tip_amount      DECIMAL(10, 2),
          product_id      STRING,
          quantity        INT
        )
        USING DELTA
        LOCATION '{self.config.bronze_path + "/invoices"}';
      """
      return query_create_bronze_table_invoices

    elif table_name == "expenses":
      query_create_bronze_table_expenses = f"""
        CREATE TABLE IF NOT EXISTS `{self.config.catalog}`.`{self.config.bronze_db}`.expenses (
          expense_id      STRING,
          description     STRING,
          store_id        STRING,
          expense_type    STRING,
          amount          DECIMAL(10, 2),
          expense_date    DATE
          )
          USING DELTA
          LOCATION '{self.config.bronze_path + "/expenses"}';
      """
      return query_create_bronze_table_expenses

    elif table_name == "stores":
      query_create_bronze_table_stores = f"""
          CREATE TABLE  IF NOT EXISTS `{self.config.catalog}`.`{self.config.bronze_db}`.stores (
              store_id STRING,
              store_name STRING,
              area STRING,
              city STRING,
              pincode STRING,
              manager_name STRING,
              opening_date DATE
          )
          USING DELTA
          LOCATION '{self.config.bronze_path + "/stores"}';
      """
      return query_create_bronze_table_stores

    elif table_name == "products":
      query_create_bronze_table_products = f"""
        CREATE TABLE IF NOT EXISTS `{self.config.catalog}`.`{self.config.bronze_db}`.products (
            product_id STRING,
            product_name STRING,
            category STRING,
            unit STRING,
            price_per_unit DECIMAL(10, 2)
        )
        USING DELTA
        LOCATION '{self.config.bronze_path + "/products"}';
      """
      return query_create_bronze_table_products

    elif table_name == "datedimension":
      query_create_bronze_table_date_dimension = f"""
        CREATE TABLE IF NOT EXISTS `{self.config.catalog}`.`{self.config.bronze_db}`.datedimension (
          date DATE,
          year INT,
          quarter INT,
          month INT,
          month_name STRING,
          week_of_year INT,
          day_of_month INT,
          day_of_week INT,
          day_name STRING,
          is_weekend BOOLEAN,
          is_month_start BOOLEAN,
          is_month_end BOOLEAN
        )
        USING DELTA
        LOCATION '{self.config.bronze_path + "/datedimension"}';
      """
      return query_create_bronze_table_date_dimension

    else:
      raise RuntimeError(f"Invalid Table Name: {table_name}")
      return None


# COMMAND ----------

class SilverMetadata:

  def __init__(self, config):
    self.config = config

  def getDatabaseCreateQuery(self):
    return f"CREATE DATABASE IF NOT EXISTS `{self.config.catalog}`.`{self.config.silver_db}`"

  def getTableCreateQuery(self, table_name):
    if table_name == "invoices":
      query_create_silver_table_invoices = f"""
        CREATE TABLE IF NOT EXISTS `{self.config.catalog}`.`{self.config.silver_db}`.invoices (
          invoice_id STRING,
          store_name STRING,
          payment_method STRING,
          tip_amount DECIMAL(10, 2),
          quantity INT,
          product_name STRING,
          category STRING,
          unit STRING,
          price_per_unit DECIMAL(10, 2),
          area STRING,
          total_price DECIMAL(10, 2),
          bill_date DATE,
          bill_timestamp TIMESTAMP,
          day_of_month INT,
          month_name STRING,
          quarter INT,
          year INT,
          week_of_year INT,
          is_weekend BOOLEAN
      )
        USING DELTA
        LOCATION '{self.config.silver_path + "/invoices"}';
      """
      return query_create_silver_table_invoices

    elif table_name == "expenses":
      query_create_silver_table_expenses = f"""
        CREATE TABLE IF NOT EXISTS `{self.config.catalog}`.`{self.config.silver_db}`.expenses (
            expense_date DATE,
            store_name STRING,
            expense_type STRING,
            amount DECIMAL(10, 2),
            day_of_month INT,
            month_name STRING,
            year INT,
            quarter INT,
            week_of_year INT
        )
        USING DELTA
        LOCATION '{self.config.silver_path + "/expenses"}';
      """
      return query_create_silver_table_expenses

    elif table_name == "products":
      query_create_silver_table_products = f"""
        CREATE TABLE IF NOT EXISTS `{self.config.catalog}`.`{self.config.silver_db}`.products (
            product_id STRING,
            product_name STRING,
            category STRING,
            unit STRING,
            price_per_unit DECIMAL(10, 2),
            start_date DATE,
            end_date DATE
        )
        USING DELTA
        LOCATION '{self.config.silver_path + "/products"}';
      """
      return query_create_silver_table_products

    elif table_name == "stores":
      query_create_bronze_table_stores = f"""
        CREATE TABLE  IF NOT EXISTS `{self.config.catalog}`.`{self.config.silver_db}`.Stores (
            store_id STRING,
            store_name STRING,
            area STRING,
            city STRING,
            pincode STRING,
            manager_name STRING,
            opening_date DATE,
            start_date DATE,
            end_date DATE
        )
        USING DELTA
        LOCATION '{self.config.silver_path + "/Stores"}';
      """
      return query_create_bronze_table_stores

    else:
      raise RuntimeError(f"Invalid Table Name: {table_name}")
      return None

# COMMAND ----------

class GoldMetadata:

  def __init__(self, config):
    self.config = config

  def getDatabaseCreateQuery(self):
    return f"CREATE DATABASE IF NOT EXISTS `{self.config.catalog}`.`{self.config.gold_db}`"

  def getTableCreateQuery(self, table_name):
    if table_name == "weekly_store_sales":
      query_create_gold_table_weekly_store_sales = f"""
        CREATE TABLE IF NOT EXISTS `{self.config.catalog}`.`{self.config.gold_db}`.weekly_store_sales (
          store_name STRING,
          week_of_year INT,
          year INT,
          total_sales_amount DECIMAL(10,2),
          total_orders INT,
          total_items_sold INT,
          avg_bill_amount DECIMAL(10,2),
          day_type STRING
        )
        USING DELTA
        LOCATION '{self.config.gold_path + "/weekly_store_sales"}';
      """
      return query_create_gold_table_weekly_store_sales

    elif table_name == "weekly_store_summary":
      query_create_gold_table_weekly_store_summary = f"""
        CREATE TABLE IF NOT EXISTS `{self.config.catalog}`.`{self.config.gold_db}`.weekly_store_summary (
          store_name STRING,
          week_of_year INT,
          year INT,
          total_sales_amount DECIMAL(10,2),
          total_expenses DECIMAL(10,2),
          net_profit DECIMAL(10,2),
          profit_margin_percentage DECIMAL(10,2)
        )
        USING DELTA
        LOCATION '{self.config.gold_path + "/weekly_store_summary"}';
      """
      return query_create_gold_table_weekly_store_summary

    elif table_name == "weekly_category_payment_time_summary":
      query_create_gold_table_weekly_category_payment_time_summary = f"""
        CREATE TABLE IF NOT EXISTS `{self.config.catalog}`.`{self.config.gold_db}`.weekly_category_payment_time_summary (
          store_name STRING,
          week_of_year INT,
          year INT,
          food_category STRING,
          payment_method STRING,
          time_of_day STRING,
          total_sales_amount DECIMAL(10,2),
          total_transactions INT
        )
        USING DELTA
        LOCATION '{self.config.gold_path + "/weekly_category_payment_time_summary"}';
      """
      return query_create_gold_table_weekly_category_payment_time_summary

    else:
      raise RuntimeError(f"Invalid Table Name: {table_name}")
      return None
