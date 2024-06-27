import requests
import pandas as pd
import psycopg2
from io import BytesIO
from sqlalchemy import create_engine


def download_excel(url):
    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"Error downloading Excel file: {e}")
        return None


def read_excel(content):
    try:
        df = pd.read_excel(BytesIO(content), engine='xlrd', skiprows=7, sheet_name=None)
        return df
    except Exception as e:
        print(f"exception {e}")


def clean_data(df):
    try:
        df= df.drop_duplicates()
        df = df.fillna(0)
        float_columns = df.select_dtypes(include=['float64'])
        print(df)
        for col in float_columns:
            df[col] = round(df[col] * 100, 2)
            df[col] = df[col].astype(str) + '%'
        return df
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return None


def main(url):
    file_content = download_excel(url)
    if not file_content:
        return
    # Read Excel file into pandas DataFrame
    df = read_excel(file_content)
    if df is None:
        return

    try:
        conn = psycopg2.connect(
            database="Corporate_Governance", user='postgres', password='0705', host='localhost', port='5432')
        conn.autocommit = True
        cursor = conn.cursor()
        print("Database connected successfully")

        connection_string = "postgresql+psycopg2://postgres:0705@localhost:5432/Corporate_Governance"
        engine = create_engine(connection_string)
        print("Engine created")

        for sheet_name, df in df.items():
            table_name = input(f"enter table name for sheet'{sheet_name}:")
            df = clean_data(df)
            print(f"cleaned data for sheet '{sheet_name}")
            df.to_sql(table_name,engine,if_exists='replace',index=False)
            print(f"data frame is written to {table_name}")
            sql_query=f"select * from {table_name};"
            data = pd.read_sql_query(sql_query,engine)
            print(data)
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return
    finally:
        if conn is not None:
            conn.close()
            print("DataBase is closed")


if __name__ == "__main__":
    # Example usage
    excel_url = input("Enter you URL to get the data ")  # Replace with actual URL
    main(excel_url)
