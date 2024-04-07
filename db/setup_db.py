
import json
import os
import sqlite3
from sqlite3 import Error
from dotenv import find_dotenv
from dotenv import dotenv_values

##########################################################################################
# SQLite3 functions
##########################################################################################
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def create_jd(conn, jd):
    """
    Create a new task
    :param conn:
    :param jd:
    :return:
    """

    sql = ''' INSERT INTO job_descriptions(id, text, offsets, filename)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, jd)
    conn.commit()

    return cur.lastrowid


##########################################################################################
# Local data
##########################################################################################

def load_jsonl_data(path):
    return [json.loads(x.strip()) for x in open(path)]

def populate_jd_table(data, conn):
    for d in data:
        # offsets = f"{d["label"]}"
        offsets = json.dumps({"sections": d["label"]})
        jd = (d["id"], d["filename"], d["data"], offsets)
        create_jd(conn, jd)


def main():
    root_dir = os.path.dirname(find_dotenv())
    db_path = os.path.join(root_dir, "db", "sqlite.db")
    inital_data_path = os.path.join(root_dir, "data", "jdsections.jsonl")
    data = load_jsonl_data(inital_data_path)

    # create a database connection
    conn = create_connection(db_path)


    sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS projects (
                                        id integer PRIMARY KEY,
                                        name text NOT NULL,
                                        begin_date text,
                                        end_date text
                                    ); """

    sql_create_tasks_table = """CREATE TABLE IF NOT EXISTS tasks (
                                    id integer PRIMARY KEY,
                                    name text NOT NULL,
                                    priority integer,
                                    status_id integer NOT NULL,
                                    project_id integer NOT NULL,
                                    begin_date text NOT NULL,
                                    end_date text NOT NULL,
                                    FOREIGN KEY (project_id) REFERENCES projects (id)
                                );"""

    sql_create_jd_table =  """ CREATE TABLE IF NOT EXISTS job_descriptions (
                                        id integer PRIMARY KEY,
                                        filename text,
                                        text text NOT NULL,
                                        html text,
                                        json text,
                                        offsets text,
                                        markup text,
                                        processed int,
                                        is_correct int
                                    ); """

    with conn:
        # create table
        print(f"creating table job_descriptions...")
        create_table(conn, sql_create_jd_table)
        print(f"populating job_descriptions...")
        populate_jd_table(data, conn)
        print("done")

 
if __name__ == '__main__':
    main()


