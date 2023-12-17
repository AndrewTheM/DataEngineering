import duckdb

def df_to_parquet(name, df):
    df.to_parquet(name, index=False, engine='fastparquet')
    
def report_car_count_per_city(conn):
    df_to_parquet("reports/1_car_count_per_city.parquet",
        conn.execute("SELECT City, COUNT(*) AS Count FROM electric_cars GROUP BY City").fetchdf())

def report_top3_cars(conn):
    df_to_parquet("reports/2_top3_cars.parquet",
        conn.execute("SELECT Model, COUNT(*) AS Count FROM electric_cars GROUP BY Model ORDER BY Count DESC LIMIT 3").fetchdf())

def report_top_car_per_postal_code(conn):
    df_to_parquet("reports/3_top_car_per_postal_code.parquet",
        conn.execute("SELECT 'Postal Code', ANY_VALUE(Make) AS Make, ANY_VALUE(Model) AS Model FROM electric_cars GROUP BY 'Postal Code'").fetchdf())

def report_car_count_by_year(conn):
    df_to_parquet("reports/4_car_count_by_year.parquet",
        conn.execute("SELECT 'Model Year', COUNT(*) AS Count FROM electric_cars GROUP BY 'Model Year'").fetchdf())

def main():
    conn = duckdb.connect(database=":memory:", read_only=False)
    
    # Create table from SQL script
    with open("sql/electric_cars.sql", "r") as f:
        conn.execute(f.read())
    
    # Import data from CSV
    conn.execute("COPY electric_cars FROM 'data/Electric_Vehicle_Population_Data.csv'")

    report_car_count_per_city(conn)
    report_top3_cars(conn)
    report_top_car_per_postal_code(conn)
    report_car_count_by_year(conn)
    conn.close()

if __name__ == "__main__":
    main()