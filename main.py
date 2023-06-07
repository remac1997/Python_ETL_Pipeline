import pypyodbc as odbc
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select, schema
import psycopg2



def extract():
    DRIVER_NAME = 'SQL SERVER'
    SERVER_NAME = 'BAHCND1345N9G\SQLEXPRESS'
    DATABASE_NAME = 'AdventureWorks2014'

    connection_string = f"DRIVER={{{DRIVER_NAME}}};" \
                        f"SERVER={SERVER_NAME};" \
                        f"DATABASE={DATABASE_NAME};" \
                        f"Trust_Connection=yes;" \

    conn = odbc.connect(connection_string)

    cursor = conn.cursor()

    query = "select name from sys.tables where name in " \
            "('EmployeeDepartmentHistory', 'Shift', 'JobCandidate'," \
            " 'EmployeePayHistory' );"


    read_table = cursor.execute(query)

    data = read_table.fetchall()
    df_list = []
    for row in data:
        query = f"select * from HumanResources.{row[0]}"
        df = pd.read_sql(query, conn)
        df_list.append(df)


    conn.close()
    return df_list









def load(df_list, table_name): # load multiple tables into postgres
    host = "localhost"
    port = "5432"
    database = "postgres"
    password = "mysecretpassword"
    username = "postgres"
    rows_imported = 0
    engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")
    #save data to postgress
    for i in range(len(df_list)):
        print(f"importing rows {rows_imported} to {rows_imported + len(df_list[i])} ... for table {table_name[i]}")
        df_list[i].to_sql(f'{table_name[i]}', engine, if_exists='replace', schema="AdventureWorks2014", index=False)
        rows_imported += len(df_list[i])
        print("Data imported successful")

table_name = ["HumanResources_EmployeeDepartmentHistory", "HumanResources_EmployeePayHistory",
            "HumanResources_JobCandidate", "HumanResources_Shift"]


extract()
print(load(extract(), table_name))




