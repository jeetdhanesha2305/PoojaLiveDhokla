from common.config import Config
from common.metadata import MetadataFactory 
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--env", required=True)
parser.add_argument("--catalog", required=True)
args = parser.parse_args()

env = args.env
catalog = args.catalog

print(f"[POOJA_LIVE_DHOKLA_LOGS] ENV: {env}")
print(f"[POOJA_LIVE_DHOKLA_LOGS] CATALOG: {catalog}")

class Setup:

  def __init__(self, config, layer):
    self.layer = layer
    if layer == "Bronze":
      self.metadata = MetadataFactory.get_metadata_instance(config, "bronze")
    elif layer == "Silver":
      self.metadata = MetadataFactory.get_metadata_instance(config, "silver")
    elif layer == "Gold":
      self.metadata = MetadataFactory.get_metadata_instance(config, "gold")
    else:
      raise ValueError(f"{layer} is not a valid layer name")

  def createDatabase(self):
    try:
      print(f"[POOJA_LIVE_DHOKLA_LOGS] Creating {self.layer} Layer Database...", end="")
      query = self.metadata.getDatabaseCreateQuery()
      spark.sql(query)
      print(f"Done")
    except Exception as e:
      raise Exception(f"[POOJA_LIVE_DHOKLA_LOGS] Failed To Create {self.layer} Database: {e}")

  def createTable(self, table_name):
    try:
      print(f"[POOJA_LIVE_DHOKLA_LOGS] Creating {self.layer} Layer Table - {table_name}...", end="")
      query = self.metadata.getTableCreateQuery(table_name)
      spark.sql(query)
      print(f"Done")
    except Exception as e:
      raise Exception(f"[POOJA_LIVE_DHOKLA_LOGS] Failed To Create {self.layer} Table - {table_name}: {e}")


  def setup(self):
        self.createDatabase()
        if self.layer == "Gold":
            table_list = [
                "weekly_store_sales",
                "weekly_store_summary",
                "weekly_category_payment_time_summary"
            ]
        elif self.layer == "Silver":
            table_list = [
                "invoices",
                "products",
                "stores",
                "expenses"
            ]
        elif self.layer == "Bronze":
            table_list = [
                "invoices",
                "products",
                "stores",
                "expenses",
                "datedimension"
            ]
        else:
            raise ValueError(f"No table setup defined for layer: {self.layer}")

        for table in table_list:
            self.createTable(table)



config = Config(env, catalog)

bronze_setup = Setup(config, "Bronze")
bronze_setup.setup()

silver_setup = Setup(config, "Silver")
silver_setup.setup()

gold_setup = Setup(config, "Gold")
gold_setup.setup()