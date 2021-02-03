import glob, os
import logging
import psycopg2
from sqlalchemy import create_engine
import pandas as pd

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
log = logging.getLogger(__name__)


class DBIn:
    def __init__(self):
        self.db_name = "northwind"
        self.csv_folder_path = "/data"
        self.db_conn = psycopg2.connect(f"host=db dbname={self.db_name} user={user} password={password}")
        self.db_cursor = self.db_conn.cursor()

    def get_tables(self, date=None):
        """ Retrieve all tables from the database and return a list of tables"""

        t_schema = "public"
        s = ""
        s += "SELECT"
        s += " table_schema"
        s += ", table_name"
        s += " FROM information_schema.tables"
        s += " WHERE"
        s += " ("
        s += " table_schema = '" + t_schema + "'"
        s += " )"
        s += " ORDER BY table_schema, table_name;"

        log.info(f"Retrieving tables from {self.db_name} database...")
        self.db_cursor.execute(s)
        list_tables = self.db_cursor.fetchall()

        return [table for _,table in list_tables]

    def generate_csv_from_table(self, table, date):
        """ Generate a csv output from an input table """ 
        s = ""
        s += "SELECT *"
        s += " FROM "
        s += f"{table}"
        s += ""
        try:
            log.info(f"Converting table {table} to csv...")
            SQL_for_file_output = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(s)
            filename = f"data/postgres/{table}/{date}/{table}.csv"
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, "w") as f:
                self.db_cursor.copy_expert(SQL_for_file_output, f)
        except Exception as e:
            log.error(f"Error: {e}")

    def generate_csv_from_file(self,date, file):
        """ Generate a csv output from a csv inside the input folder """  
        try:
            log.info(f"Reading {file}...")
            output = f"data/csv/{file.split('.',1)[0]}/{date}/{file}"
            os.makedirs(os.path.dirname(output), exist_ok=True)
            w = open(output, "w")
            with open(f"{self.csv_folder_path}/{file}", "r") as f:
                header = f.readline()
                w.write(header)
                for row in f:
                    w.write(row)
            w.close()
        except Exception as e:
            log.error(f"Error: {e}")


class DBOut:
    def __init__(self):
        self.engine = create_engine(f'postgresql://{user}:{password}@db-output:5432/pipeout')
    
    def csv_to_table(self, date):
        """ Populate the tables with the info contained in the csv input files """
        try:
            for filename in glob.iglob(f'./data/*/*/{date}/*.csv', recursive=True):
                log.info(f"Reading {filename}...")
                table_name = filename.split("/")[-1].split(".")[0]
                df = pd.read_csv(filename)
                try:
                    log.info(f"Writing {table_name}...")
                    df.to_sql(table_name, self.engine, index=False, if_exists='append')
                except Exception as e:
                    log.error(f"Error: {e}")
        except Exception as e:
            log.error(f"Error: {e}")

        return "success"

    def apply_constraints(self):
        """ Apply foreign key constraints to the output database"""
        try:
            log.info(f"Applying constraints to output database...")
            with self.engine.connect() as con:
                file = open("/data/output_constraints.sql")
                con.execute(file.read())
            return "Success. Constraints applied"
        except Exception as e:
            log.error(f"Error: {e}")



    def run_query(self, query):
        """ Run a given query on the output database"""
        try:
            log.info(f"Running query: {query}")
            with self.engine.connect() as con:
                return con.execute(query)
        except Exception as e:
            log.error(f"Error: {e}")


