
class Cleanup:
  
  def __init__(self, config):
    self.config = config
  
  def dropDatabase(self, db_name):
    try:
      print(f"[POOJA_LIVE_DHOKLA_LOGS] Dropping Database {db_name}...") 
      spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE") 
      print(f"Done")
    except Exception as e:
      print(f"Failed")
      print(e)
  
  def dropTable(self, tbl_name):
    try:
      print(f"[POOJA_LIVE_DHOKLA_LOGS] Dropping Table {tbl_name}...") 
      spark.sql(f"DROP TABLE IF EXISTS {tbl_name}")
      print(f"Done")
    except Exception as e:
      print(f"Failed")
      print(e) 
    
  def clearDeltaLakePath(self, tbl_path):
    try:
      print(f"[POOJA_LIVE_DHOKLA_LOGS] Clearing Table Path {tbl_path}...") 
      dbutils.fs.rm(tbl_path, True)
      print(f"Done")
    except Exception as e:
      print(f"Failed")
      print(e) 
  
  def cleanBronzeLayer(self):
      self.dropTable(f"`{self.config.catalog}`.`{self.config.bronze_db}`.`invoices`")
      self.dropTable(f"`{self.config.catalog}`.`{self.config.bronze_db}`.`datedimension`")
      self.dropTable(f"`{self.config.catalog}`.`{self.config.bronze_db}`.`expenses`")
      self.dropTable(f"`{self.config.catalog}`.`{self.config.bronze_db}`.`products`")
      self.dropTable(f"`{self.config.catalog}`.`{self.config.bronze_db}`.`stores`")
      self.dropDatabase(f"`{self.config.catalog}`.`{self.config.bronze_db}`")
      self.clearDeltaLakePath(self.config.bronze_path)
  
  def cleanSilverLayer(self):
      self.dropTable(f"`{self.config.catalog}`.`{self.config.silver_db}`.`invoices`")
      self.dropTable(f"`{self.config.catalog}`.`{self.config.silver_db}`.`expenses`")
      self.dropTable(f"`{self.config.catalog}`.`{self.config.silver_db}`.`products`")
      self.dropTable(f"`{self.config.catalog}`.`{self.config.silver_db}`.`stores`")
      self.dropDatabase(f"`{self.config.catalog}`.`{self.config.silver_db}`")
      self.clearDeltaLakePath(self.config.silver_path)
  
  def cleanGoldLayer(self):
      self.dropTable(f"`{self.config.catalog}`.`{self.config.gold_db}`.`weekly_store_sales`")
      self.dropTable(f"`{self.config.catalog}`.`{self.config.gold_db}`.`weekly_store_summary`")
      self.dropTable(f"`{self.config.catalog}`.`{self.config.gold_db}`.`weekly_category_payment_time_summary`")
      self.dropTable(f"`{self.config.catalog}`.`{self.config.gold_db}`.`stores`")
      self.dropDatabase(f"`{self.config.catalog}`.`{self.config.gold_db}`")
      self.clearDeltaLakePath(self.config.gold_path)