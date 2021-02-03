import os
from flask import Flask, jsonify
import psycopg2
import logging
from . import db_worker
from .util import find_csv_filenames

log = logging.getLogger(__name__)

def create_app():
    app = Flask(__name__)
    logging.basicConfig(filename='pipelog.log', encoding='utf-8', level=os.getenv("LOG_LEVEL"),
                        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    origin_db= db_worker.DBIn()
    destination_db = db_worker.DBOut()

    @app.route("/")
    def main():
        return "Pipe-Service"

    @app.route("/list_input")
    def list_input():
        """ List all input resources"""

        try:
            tables = origin_db.get_tables()
            csv = find_csv_filenames("/data")
            return jsonify({"sql_tables":tables, "csv_files": csv})
        except Exception as e:
            return f"Something went wrong... {e}"

    @app.route("/input_to_csv/<date>")
    def input_to_csv(date):
        """ Convert the input resources to csv ffor a given date"""

        try:
            # convert tables
            for table in origin_db.get_tables():
                origin_db.generate_csv_from_table(table, date)

            # convert csv
            for csv in find_csv_filenames("/data"):
                origin_db.generate_csv_from_file(date, csv)

            return "Success"

        except Exception as e:
            return f"Something went wrong... {e}"

    @app.route("/run_pipe/<date>")
    def pipe_run(date):
        """ Run the pipe of a given date """
        log.info(f"Running pipe... {date}")
        try:
            # convert input csv files and tables from the origin datasource to csv
            input_to_csv(date)

            # convert the csv files to tables on the destination database
            destination_db.csv_to_table(date)

            return "Success"
       
        except Exception as e:
            log.error(f"Something went wrong... {e}")
            return f"Something went wrong... {e}"
        


    @app.route("/apply_constraints")
    def pipe_apply_contraints():
        """ Apply foreign key constraints to the output database """
        
        log.info("Applying foreign key constraints")
        try:
            # apply foreign key constraints to the output database
            destination_db.apply_constraints()

            return "Success"

        except Exception as e:
            log.error(f"Something went wrong... {e}")
            return f"Something went wrong... {e}"


    @app.route("/order_details")
    def order_details():
        """ Return the all the orders and its details """

        try:
            result = destination_db.run_query(
                "select * from orders join order_details on (orders.order_id=order_details.order_id)"
            )

            return jsonify({'orders': [dict(row) for row in result]})
        
        except Exception as e:
            log.error(f"Something went wrong... {e}")
            return f"Something went wrong... {e}"


    return app

    