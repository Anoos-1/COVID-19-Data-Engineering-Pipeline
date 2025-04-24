from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook


base_path = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(base_path, "data")
transformed_path = os.path.join(data_path, "transformed.csv")

def create_tables():
    hook = PostgresHook(postgres_conn_id='Postgres_conn')
    conn = hook.get_conn()
    cur = conn.cursor()
    
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_location (
            location_id SERIAL PRIMARY KEY,
            country TEXT,
            province TEXT,
            UNIQUE(country, province)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id SERIAL PRIMARY KEY,
            full_date DATE UNIQUE,
            day INT,
            month INT,
            year INT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_covid_cases (
            case_id SERIAL PRIMARY KEY,
            location_id INT REFERENCES dim_location(location_id),
            date_id INT REFERENCES dim_date(date_id),
            confirmed INT,
            deaths INT,
            recovered INT
        );
    """)
    
    
    conn.commit()
    cur.close()
    conn.close()

def extract_data():
    df_confirmed = pd.read_csv(os.path.join(data_path, "confirmed_data.csv")).iloc[:, :50]
    df_deaths = pd.read_csv(os.path.join(data_path, "deaths_data.csv"))
    df_recovered = pd.read_csv(os.path.join(data_path, "recovered_data.csv"))

    df_confirmed.to_csv(os.path.join(data_path, "df_confirmed_temp.csv"), index=False)
    df_deaths.to_csv(os.path.join(data_path, "df_deaths_temp.csv"), index=False)
    df_recovered.to_csv(os.path.join(data_path, "df_recovered_temp.csv"), index=False)

def transform_data():
    df_confirmed = pd.read_csv(os.path.join(data_path, "df_confirmed_temp.csv"))
    df_deaths = pd.read_csv(os.path.join(data_path, "df_deaths_temp.csv"))
    df_recovered = pd.read_csv(os.path.join(data_path, "df_recovered_temp.csv"))

    df_confirmed = df_confirmed.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
                                     var_name='date', value_name='confirmed')
    df_deaths = df_deaths.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
                               var_name='date', value_name='deaths')
    df_recovered = df_recovered.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], 
                                     var_name='date', value_name='recovered')

    df = df_confirmed.merge(df_deaths, on=['Province/State', 'Country/Region', 'Lat', 'Long', 'date'])
    df = df.merge(df_recovered, on=['Province/State', 'Country/Region', 'Lat', 'Long', 'date'])


    df['date'] = pd.to_datetime(df['date'])
    df.rename(columns={'Province/State': 'province', 'Country/Region': 'country'}, inplace=True)
    df.fillna({'province': 'Unknown'}, inplace=True)

    df.to_csv(transformed_path, index=False)

def load_data():
    df = pd.read_csv(transformed_path)
    df['date'] = pd.to_datetime(df['date'])

    hook = PostgresHook(postgres_conn_id='Postgres_conn')
    conn = hook.get_conn()
    cur = conn.cursor()


    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO dim_location (country, province)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            RETURNING location_id
        """, (row['country'], row['province']))
        loc_id = cur.fetchone()
        if loc_id is None:
            cur.execute("SELECT location_id FROM dim_location WHERE country=%s AND province=%s", 
                        (row['country'], row['province']))
            loc_id = cur.fetchone()
        loc_id = loc_id[0]

        date = row['date']
        cur.execute("""
            INSERT INTO dim_date (full_date, day, month, year)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING date_id
        """, (date, date.day, date.month, date.year))
        date_id = cur.fetchone()
        if date_id is None:
            cur.execute("SELECT date_id FROM dim_date WHERE full_date=%s", (date,))
            date_id = cur.fetchone()
        date_id = date_id[0]

        cur.execute("""
            INSERT INTO fact_covid_cases (location_id, date_id, confirmed, deaths, recovered)
            VALUES (%s, %s, %s, %s, %s)
        """, (loc_id, date_id, int(row['confirmed']), int(row['deaths']), int(row['recovered'])))


    conn.commit()
    cur.close()
    conn.close()

with DAG('covid_etl_dag',
         schedule_interval=None,
         start_date=datetime(2025, 4, 24),
         catchup=False) as dag:

    create_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables
    )

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    create_task >> extract_task >> transform_task >> load_task
