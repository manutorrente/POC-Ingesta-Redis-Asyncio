from redis import Redis
import msgspec
import os
from dotenv import load_dotenv
from impala.dbapi import connect
import logging
import time
import asyncio

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv(override=True)

encoder = msgspec.json.Encoder()

impala_conf = {
        "host" : os.getenv("IMPALA_HOST"),
        "port" : 21050,
        "user" : os.getenv("IMPALA_USERNAME"),
        "password" : os.getenv("IMPALA_PASSWORD"),
        "timeout" : 600
    }

redis_conf = {
    "host" : os.getenv("REDIS_HOST"),
    "port" : 6379,
    "db" : 0,
    "username" : os.getenv("REDIS_USERNAME"),
    "password" : os.getenv("REDIS_PASSWORD"),
}

def init_impala_connection():
    try:
        impala_conn = connect(host=impala_conf["host"],
                    port=impala_conf["port"],
                    user=impala_conf["user"],
                    password=impala_conf["password"],
                timeout=impala_conf["timeout"]
                )
        return impala_conn
    except Exception as e:
        raise e

def init_redis_connection():
    try:
        redis_cli = Redis(
            host=redis_conf["host"],
            port=redis_conf["port"],
            db=redis_conf["db"],
            username=redis_conf["username"],
            password=redis_conf["password"],
            decode_responses=True
        )
        return redis_cli
    except Exception as e:
        raise e


def build_json(data: list, data_columns: list[str], remove_nulls: bool = True) -> str:

    pairs = zip(data_columns, data)
    if remove_nulls:
        dict_data = {col: str(val) for col, val in pairs if val is not None}
    else:
        dict_data = {col: str(val) for col, val in pairs}
    
    return encoder.encode(dict_data)#.decode('utf-8')
        
key_column = 1

class TableIterator:
    def __init__(self, impala_conn, table_name: str, chunk_size: int = 10000) -> None:
        self.conn = impala_conn
        self.cursor = impala_conn.cursor()
        query = f"SELECT * FROM {table_name}"
        logger.info(f"Executing query {query}")
        self.cursor.execute(query)
        self.chunk_size = chunk_size
        self.table_name = table_name

    def __iter__(self):
        return self

    def __next__(self):
        logger.info(f"Fetching next chunk of data from table {self.table_name}")
        rows = self.cursor.fetchmany(self.chunk_size)
        logger.info(f"Fetched {len(rows)} rows from table {self.table_name}")
        if not rows:
            self.cursor.close()
            raise StopIteration
        return rows

    def get_column_names(self) -> list[str]:
        cursor = self.conn.cursor()
        cursor.execute(f"DESCRIBE {self.table_name}")
        columns = [row[0] for row in cursor.fetchall()]
        logger.info(f"Fetched column names for table {self.table_name}")
        cursor.close()
        return columns
                
def insert_table(table_name: str, redis_cli: Redis, impala_conn) -> None:
    
    iterator = TableIterator(impala_conn, table_name)

    column_names: list[str] = iterator.get_column_names()
    column_names.pop(key_column)
    
    for chunk in iterator:
        
        pipeline = redis_cli.pipeline()
        
        for row in chunk:
            key = f"tmp:{table_name}:{row[key_column]}"
            row_list = list(row)
            row_list.pop(key_column)
            json_data = build_json(row_list, column_names)
            pipeline.set(key, json_data, ex=86400)

        logger.info(f"Inserted chunk into Redis for table {table_name}")
        pipeline.execute()


async def main():
    logger.info("Hello from redis-poc-async!")
    
    impala_conn = init_impala_connection()
    redis_cli = init_redis_connection()

    start_time = time.time()
    
    """     tables = ["de_bsj_3ref.riesgos_calificacion_prestamos_tarjetas_snap", 
              "de_bsj_3ref.pre_bureau_antecedentes_negativos_om", 
              "de_bsj_3ref.dim_veraz",
              "de_gpn_3ref.productos_crediticios_gp",
              "de_gpn_3ref.pre_bureau_fuentes_publicas_om"] """
    
    tables = ["pr_bsf_3ref.riesgos_calificacion_prestamos_tarjetas_snap", "pr_bsf_3ref.pre_bureau_antecedentes_negativos_om", "pr_bsf_3ref.dim_veraz"]
        
    coros = [asyncio.to_thread(insert_table, table, redis_cli, impala_conn) for table in tables]
    await asyncio.gather(*coros)
        

    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f"Data ingestion completed in {elapsed_time:.2f} seconds.")

    logger.info("Closing connections.")
    impala_conn.close()
    redis_cli.close()
    
if __name__ == "__main__":
    asyncio.run(main())