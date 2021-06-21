# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import sqlite3
from sqlite3 import Error
import csv


def create_connection(path):
    """
    :returns connection if the database connection make successfully
    :param path: str
    """
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection


def create_tables(conn):
    """
    creates transaction and users table with specified columns
    :param conn: connection
    """
    conn.execute("""DROP TABLE IF EXISTS transactions;""")
    conn.execute("""CREATE TABLE transactions (
            transaction_id INT ,
            date DATE,
            user_id INT,
            is_blocked TEXT,
            transaction_amount REAL,
    		transaction_category_id INT);""")
    conn.execute("""DROP TABLE IF EXISTS users;""")
    conn.execute("""CREATE TABLE users (
            user_id INT ,
            is_active TEXT);""")
    print("Table created successfully")


def read_file(conn, filename):
    """
    Read CSV files from data folder and writes into Db
    :param conn: Connection
    :param filename: Str
    """
    cur = conn.cursor()
    filepath = ".\data\\" + filename + ".csv"

    print("File Read Started for {}.csv ".format(filename))
    with open(filepath) as f:
        reader = csv.reader(f)
        ncol = len(next(reader))  # Read first line and count columns
        cols = "?," * (ncol - 1) + "?"
        query = "INSERT INTO {} VALUES ({});".format(filename, cols)
        for field in reader:
            # in order to skip the lines that is empty
            if len(field) != ncol:
                continue
            cur.execute(query, field)
    print("File Read Complete for {}.csv ".format(filename))
    conn.commit()


def write_csv(data, fileName):
    """
    Writes data to csv
    :param data: []
    :param fileName: Str
    """
    print("File {}.csv Write Started  ".format(fileName))
    csvWriter = csv.writer(open(fileName + ".csv", "w"))
    csvWriter.writerows(data)
    print("File {}.csv Write Finished".format(fileName))


if __name__ == '__main__':
    # Task 1: Write a program which computes the result of this one
    # specific query.
    # Db connections, create table dmls and reading from files handled
    path = ".\\transaction.db"
    conn = create_connection(path)
    create_tables(conn)
    read_file(conn, "transactions")
    read_file(conn, "users")

    # Executing the query and writting to CSV
    cursor = conn.cursor()
    cursor.execute("""SELECT
                    t.transaction_category_id,
                    sum(t.transaction_amount) AS sum_amount,
                    count(DISTINCT t.user_id) AS num_users
                    FROM transactions t
                    JOIN users u USING (user_id)
                    WHERE t.is_blocked = 'False'
                    AND u.is_active = 'True'
                    GROUP BY t.transaction_category_id
                    ORDER BY sum_amount DESC;""")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    write_csv(rows, "task1")

    # Task 2: to write a SQL query, which computes this table based on the transactions
    # table from the described schema.
    # SQL code for the answer
    query = """
        select t.transaction_id, t.user_id, t.date, count(t2.transaction_id) as theTrancastions from  (
            select
            t.*, DATE(t.date, '-7 DAYS') as dateMinBound
            from transactions t
            ) t 
        left join transactions t2 on 
            t.user_id = t2.user_id
            and t.date > t2.date and
            t.dateMinBound < t2.date
        group by t.transaction_id, t.user_id, t.date
        order by t.user_id desc, t.date;
        """

    # Hint for the sql query
    queryHint = "CREATE INDEX Idx1 ON transactions(user_id);"
    cursor.execute(queryHint)


    cursor.execute(query)
    rows = cursor.fetchall()
    # print(rows)
    write_csv(rows, "task2")

    conn.close()
