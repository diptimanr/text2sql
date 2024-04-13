import db_config
from sqlalchemy import create_engine, text

from llama_index.llms.ollama import Ollama
from llama_index.core import SQLDatabase
from llama_index.core.query_engine import NLSQLTableQueryEngine
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core import Settings

def get_connection():
    return create_engine(
        url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
            db_config.db_user, db_config.db_password, db_config.db_host, db_config.db_port, db_config.db_name
        )
    )
def main():
    llm = Ollama(model="duckdb-nsql", request_timeout=30.0)
    print("Selected Model :: ", llm.model)
    print("=====================")

    Settings.embed_model = HuggingFaceEmbedding(
        model_name="BAAI/bge-small-en-v1.5"
    )

    engine = get_connection()
    db_tables = ["customers","employees",
                 "offices","orderdetails",
                 "orders","payments",          
                 "productlines","products"]

    sql_database = SQLDatabase(engine, include_tables=db_tables)
    query_engine = NLSQLTableQueryEngine(sql_database=sql_database,
                                         tables=db_tables,
                                         llm=llm)
    
    #query_str = "How many customers in Las Vegas ?"
    #query_str = "Find customers with no orders"
    query_str = "Find number of orders for each status."
    response = query_engine.query(query_str)
    print("Generated Query ::>")
    print(response.metadata['sql_query'])
    print("=====================")
    print("Generated Query Result ::>")
    print(response.metadata['result'])
    print("=====================")
    print("Run generated SQL query on database ::>")
    with engine.connect() as connection:
        results = connection.execute(text(response.metadata['sql_query']))
        print(results.first())

if __name__ == '__main__':
    main()