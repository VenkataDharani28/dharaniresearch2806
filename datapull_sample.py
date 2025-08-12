
import configparser
import snowflake.connector
import pandas as pd
import logging
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class SnowflakeDataPull:

    def __init__(self, config_path='config.ini'):
        self.config_path = config_path
        self.sf_config = None
        self.conn = None
        self.cur = None

    def get_sql_query(self):
        """Reads the SQL query from the file specified in config [QUERIES] section."""
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sql_file_path = self.queries_config.get('sql_query', 'customer.sql')
        sql_file_full_path = os.path.join(base_dir, sql_file_path) if not os.path.isabs(sql_file_path) else sql_file_path
        if not os.path.exists(sql_file_full_path):
            logging.error(f"SQL file not found: {sql_file_full_path}")
            return None
        with open(sql_file_full_path, 'r', encoding='utf-8') as f:
            query = f.read()
        if not query.strip():
            logging.error("SQL file is empty. Please provide a valid query in the SQL file.")
            return None
        return query

    def load_config(self):
        logging.info(f"Looking for config file at: {os.path.abspath(self.config_path)}")
        if not os.path.exists(self.config_path):
            logging.error(f"Config file not found at {os.path.abspath(self.config_path)}")
            return False
        config = configparser.ConfigParser()
        config.read(self.config_path)
        logging.info(f"Sections found in config.ini: {config.sections()}")
        if 'SNOWFLAKE_SERVER' in config and 'SNOWFLAKE_DATAPULL' in config:
            self.sf_config = {**config['SNOWFLAKE_SERVER'], **config['SNOWFLAKE_DATAPULL']}
        else:
            logging.error("Required sections [SNOWFLAKE_SERVER] and [SNOWFLAKE_DATAPULL] not found in config.ini. Please check your configuration file.")
            return False
        # Load queries section if present
        self.queries_config = config['QUERIES'] if 'QUERIES' in config else {}
        return True

    def connect(self):
        try:
            logging.info("Connecting to Snowflake...")
            self.conn = snowflake.connector.connect(
                user=self.sf_config['user'],
                password=self.sf_config['password'],
                account=self.sf_config['account'],
                warehouse=self.sf_config['warehouse'],
                database=self.sf_config['database'],
                schema=self.sf_config['schema'],
                role=self.sf_config['role']
            )
            logging.info("Successfully connected to Snowflake.")
            self.cur = self.conn.cursor()
            return True
        except Exception as e:
            logging.error(f"Error connecting to Snowflake: {e}", exc_info=True)
            return False

    def execute_query(self, query, output_csv_path='output.csv', params=None):
        """
        Executes a query. If params is provided, passes it to the cursor for parameterized queries.
        """
        try:
            logging.info(f"Executing query: {query}")
            if params:
                self.cur.execute(query, params)
            else:
                self.cur.execute(query)
            logging.info("Query executed successfully.")
            df = self.cur.fetch_pandas_all()
            logging.info(f"Fetched {len(df)} rows from Snowflake.")
            df.to_csv(output_csv_path, index=False)
            logging.info(f"Data successfully written to {output_csv_path}")
        except Exception as e:
            logging.error(f"Error during query execution: {e}", exc_info=True)

    def close(self):
        if self.cur:
            self.cur.close()
            logging.info("Cursor closed.")
        if self.conn:
            self.conn.close()
            logging.info("Connection closed.")

def main():
    snowflake_pull = SnowflakeDataPull()
    if not snowflake_pull.load_config():
        return
    if not snowflake_pull.connect():
        return
    query = snowflake_pull.get_sql_query()
    if not query:
        snowflake_pull.close()
        return
    # Example: for a parameterized query like 'SELECT * FROM table WHERE id = %s', pass params=(123,)
    # params = (123,)
    # snowflake_pull.execute_query(query, output_csv_path='water_quality_data.csv', params=params)

    # For non-parameterized queries, just call without params:
    snowflake_pull.execute_query(query, output_csv_path='water_quality_data.csv')
    snowflake_pull.close()

if __name__ == "__main__":
    main()
