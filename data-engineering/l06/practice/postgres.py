import os
import psycopg2


pg_creds = {
    "host":     "localhost",
    "port":     "5432",
    "database": "pagila",
    "user":     "pguser",
    "password": "secret"
}

def download_from_pg_and_print():
    with psycopg2.connect(host=pg_creds['host']
                        , port=pg_creds['port']
                        , database=pg_creds['database']
                        , user=pg_creds['user']
                        , password=pg_creds['password']) as pg_connection:
        cursor = pg_connection.cursor()
        cursor.execute("SELECT * FROM actor LIMIT 2;")
        result = cursor.fetchall()
        print(result)

def download_from_pg_and_save_csv():
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with open(os.path.join('.', 'data', 'sample.csv'), 'w') as csv_file:
            cursor.copy_expert("COPY category (category_id, name) TO STDOUT WITH HEADER CSV", csv_file)


def upload_to_pg():
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with open(os.path.join('.', 'data', 'sample.csv'), 'r') as csv_file:
            cursor.copy_expert("COPY category2 (category_id, name) FROM STDIN WITH HEADER CSV", csv_file)

def insert_to_pg():
    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        cursor.execute("INSERT INTO category2 VALUES (100, 'IT fiction');")

def main():
    download_from_pg_and_print()
    download_from_pg_and_save_csv()
    upload_to_pg()
    insert_to_pg()


if __name__ == '__main__':
    main()
    