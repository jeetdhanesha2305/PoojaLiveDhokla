#
from datetime import datetime, timedelta
import pytz

class Config: 

    def __init__(self, env="", catalog=""): 
        self.medallion_path = (
            spark.sql("describe external location `medallion`").select("url").collect()[0][0]
        )

        self.catalog = catalog

        self.bronze_path = self.medallion_path + "/bronze"
        self.silver_path = self.medallion_path + "/silver"
        self.gold_path = self.medallion_path + "/gold" 

        self.bronze_db = "bronze_db"
        self.silver_db = "silver_db"
        self.gold_db = "gold_db" 

        self.scope = f"key-vault-{env}"

        # CONNECTION DETAILS
        ## azure sql connection
        self.azure_sql_database_url = dbutils.secrets.get(scope=self.scope, key="azure-sql-database-url")
        self.azure_sql_server_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

        self.azure_sql_database_username = dbutils.secrets.get(scope=self.scope, key="azure-sql-database-username")
        self.azure_sql_database_password = dbutils.secrets.get(scope=self.scope, key="azure-sql-database-password")
        

        ## azure cosmos db connection
        self.azure_cosmos_database_url = dbutils.secrets.get(scope=self.scope, key="azure-cosmos-database-url")
        self.azure_cosmos_database_key = dbutils.secrets.get(scope=self.scope, key="azure-cosmos-database-key")
        self.azure_cosmos_database_name = dbutils.secrets.get(scope=self.scope, key="azure-cosmos-database-name")
        self.azure_cosmos_database_invoices_container = dbutils.secrets.get(scope=self.scope, key="azure-cosmos-database-invoices-container")
        self.azure_cosmos_database_expenses_container = dbutils.secrets.get(scope=self.scope, key="azure-cosmos-database-expenses-container")  

        ## blob storage 
        self.blob_storage_url = dbutils.secrets.get(scope=self.scope, key="blob-storage-url")  
        self.blob_storage_key = dbutils.secrets.get(scope=self.scope, key="blob-storage-key")  
        self.blob_storage_datedimension_container = dbutils.secrets.get(scope=self.scope, key="blob-storage-datedimension-container")  
        self.date_dimension_file_name = "date_dimension_2025.csv" 


        # get yesterday's date
        ist = pytz.timezone('Asia/Kolkata')
        now_ist = datetime.now(ist)
        self.todays_date = now_ist.strftime('%Y-%m-%d')
        print(f"todays_date: {self.todays_date}")
        self.yesterdays_date = (now_ist - timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"yesterdays_date: {self.yesterdays_date}")
