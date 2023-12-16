import psycopg2

CSV = "data"
SQL = "sql"

TABLES = ["accounts", "products", "transactions"]

def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    
    for table in TABLES:
        with open(f"{SQL}/{table}.sql", "r") as sql:
            with open(f"{CSV}/{table}.csv", "r") as csv:
                cursor = conn.cursor()
                cursor.execute(sql.read())
                cursor.copy_from(csv, table, sep=',', columns=csv.readline()[:-1].split(','))
                conn.commit()
                cursor.close()
    
    conn.close()
    
if __name__ == "__main__":
    main()