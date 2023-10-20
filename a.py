# import pymssql
# import pandas as pd
# from sqlalchemy import create_engine
# from snowflake.sqlalchemy import URL
# from sample import sql, snow

# # Establish the SQL Server connection
# server = sql.get("servername")
# user_sql = sql.get("username_sql")
# password_sql = sql.get("password_sql")
# database_sql = sql.get("Database_sql")

# conn = pymssql.connect(server, user_sql, password_sql, database_sql)
# SQL_cursor = conn.cursor()
# print('Connected to SQL Server')

# account = snow.get("account")
# user_snow = snow.get("user_snow")
# password_snow = snow.get("password_snow")
# database_snow = snow.get("database_snow")
# schema_snow = snow.get("schema_snow")

# # Create a Snowflake URL and engine
# url = URL(
#     account=account,
#     user=user_snow,
#     password=password_snow,
#     database=database_snow,
#     schema=schema_snow
# )
# engine = create_engine(url)
# SnowFlake_connection = engine.connect()
# print('Connected to Snowflake')

# # Fetch table names from Snowflake
# table_query = "SHOW TABLES;"
# table_result = SnowFlake_connection.execute(table_query)
# tables = [row[0] for row in table_result]

# user_column_name = 'MODIFIEDDATETIME'
# user_date = '2023-09-15'  # Ensure the date is in the correct format 'YYYY-MM-DD'

# # Create a list to store the results of validation and data
# validation_results = []

# # Loop through tables and perform record count for the specified date
# for table_name in tables:
#     # Build Snowflake and SQL queries with date filtering
#     snow_count_query = f"select count(*) from CTG_DEV.RAW_POC.{table_name} where DATE({user_column_name}) = TO_DATE('{user_date}', 'YYYY-MM-DD')"
#     sql_count_query = f"select count(*) from {table_name}_POC where CAST({user_column_name} AS DATE) = '{user_date}'"

#     # Execute count queries in Snowflake and SQL Server
#     Snow_cursor = SnowFlake_connection.execute(snow_count_query)
#     snowflake_row_count = Snow_cursor.fetchone()[0]

#     SQL_cursor.execute(sql_count_query)
#     sql_server_row_count = SQL_cursor.fetchone()[0]

#     print(f'\nRow count for table {table_name} in Snowflake: {snowflake_row_count}')
#     print(f'Row count for table {table_name} in SQL Server: {sql_server_row_count}')

#     if snowflake_row_count != sql_server_row_count:
#         print(f'\nRecord counts do not match for Table: {table_name}')
#         break

#     # Check if record counts match
#     if snowflake_row_count == sql_server_row_count:
#         print('\nRecord counts match. Proceeding with validation.')

#         # Build Snowflake and SQL queries for data retrieval
#         snow_query = f"select * from CTG_DEV.RAW_POC.{table_name} where DATE({user_column_name}) = TO_DATE('{user_date}', 'YYYY-MM-DD')"
#         sql_query = f"select * from {table_name}_POC where CAST({user_column_name} AS DATE) = '{user_date}'"

#         # Extract table name from the Snowflake table name
#         parts = table_name.split('.')
#         Table_Name = parts[-1]

#         print('Validation Started for Table: ' + Table_Name)

#         # Snowflake table data for the specified date
#         query = snow_query
#         try:
#             test_d = pd.read_sql(query, SnowFlake_connection)
#         except Exception as e:
#             print(f"Error reading data from Snowflake for table {table_name}: {str(e)}")
#             test_d = pd.DataFrame()
#         test_snow = pd.DataFrame(test_d)

#         # SQL Server table data for the specified date
#         SQL_cursor.execute(sql_query)

#         # Fetch all rows from the cursor
#         sql_results = SQL_cursor.fetchall()

#         # Create a DataFrame from the SQL results
#         test_sql = pd.DataFrame(sql_results, columns=[desc[0] for desc in SQL_cursor.description])

#         # Handling boolean value bug of Python SQL connector
#         sql_bool_col = [x for x, y in dict(test_sql.dtypes).items() if y == 'bool']
#         if sql_bool_col:
#             for a in sql_bool_col:
#                 test_sql[a] = test_sql[a].astype(int)

#         # Validation Process for the specified date
#         check = 0
#         if not test_sql.empty:
#             test_sql = test_sql.fillna(0)
#             test_snow = test_snow.fillna(0)
#             test_sql.columns = test_snow.columns  # Set column names for SQL data
#             test_sql = test_sql.astype(test_snow.dtypes)
#             if test_snow.equals(test_sql):
#                 check = 1
#             else:
#                 list_col = test_sql.columns
#                 for a in list_col:
#                     test_sql_temp = test_sql.sort_values([a], ascending=True).reset_index(drop=True)
#                     test_snow_temp = test_snow.sort_values([a], ascending=True).reset_index(drop=True)
#                     if test_snow_temp.equals(test_sql_temp):
#                         check = 1
#                         break
#                     else:
#                         check = 0
#         else:
#             print(f'----> Table {table_name} has 0 records' + '\n')
#             check = 2

#         validation_results.append((f'{Table_Name} (Date Validation)', check, test_snow))

#     else:
#         print('Record counts do not match. Skipping validation for Table: ' + table_name)

#     if check == 1:
#         print(f'\nValidation is Done for Table: {table_name}')

#         print('\nAll Validations for the Specified Date Passed.')

# # Close the Snowflake connection
# SnowFlake_connection.close()

# # Check if any validation failed for the specified date
# failed_validations = [result for _, result, _ in validation_results if result == 0]

# if failed_validations:
#     print('\n============= Report =============')
#     print(f'Validations for the Specified Date: {user_date}')
#     for table_name, result, data in validation_results:
#         if result == 0:
#             print(f'\nTable: {table_name}')
#             print('\nValidation Failed')
#             if not data.empty:
#                 # Find the mismatched rows
#                 mismatched_data = data.merge(test_sql, indicator=True, how='outer').loc[lambda x: x['_merge'] == 'right_only']

#                 # Save the mismatched data to a CSV file
#                 csv_filename = f'Mismatched_data_{table_name}_{user_date}.csv'
#                 mismatched_data.drop('_merge', axis=1, inplace=True)
#                 mismatched_data.to_csv(csv_filename, index=False)
#                 print(f'\nMismatched data saved to: {csv_filename}')








# import pymssql
# import pandas as pd
# from sqlalchemy import create_engine
# from snowflake.sqlalchemy import URL

# # Establish the SQL Server connection
# conn = pymssql.connect("CanteenMarketDBDev", "CDC_DMS_User", "xc21kj89ik!", "Development_Market")
# SQL_cursor = conn.cursor()

# # Define Snowflake connection parameters
# snowflake_params = {
#     "account": "cgna-canteen_data",
#     "user": "RangaS01",
#     "password": "qaws90Ki@#",
#     "database": "CTG_DEV",
#     "schema": "SIGMOID_RAW",
# }

# # Create a Snowflake URL and engine
# url = URL(**snowflake_params)
# engine = create_engine(url)
# SnowFlake_connection = engine.connect()
# print('Connected to Snowflake')

# # Fetch list of tables from Snowflake
# tables_query = "SHOW TABLES"
# tables_result = SnowFlake_connection.execute(tables_query)
# Snow_tables_list = [row[1] for row in tables_result]

# # Create a list to store the results 
# # of validation and data
# user_column_name = "MODIFIEDDATETIME"
# user_date = '2019-05-04'
# validation_results = []

# # Loop through tables and perform validation for each table
# for table_name in Snow_tables_list:
#     print(f"\nStarting validation for table: {table_name}")

#     check = 0

#     # Build Snowflake and SQL queries for row count
#     snow_count_query = f"select count(*) from CTG_DEV.SIGMOID_RAW.{table_name} where DATE({user_column_name}) = '{user_date}'"
#     sql_count_query = f"select count(*) from {table_name} where CAST({user_column_name} AS DATE) = '{user_date}'"

#     # Execute count queries in Snowflake and SQL Server
#     Snow_cursor = SnowFlake_connection.execute(snow_count_query)
#     snowflake_row_count = Snow_cursor.fetchone()[0]

#     SQL_cursor.execute(sql_count_query)
#     sql_server_row_count = SQL_cursor.fetchone()[0]

#     print(f'\nRow count for table {table_name} in Snowflake: {snowflake_row_count}')
#     print(f'Row count for table {table_name} in SQL Server: {sql_server_row_count}')

#     # Check if row counts match
#     if snowflake_row_count == sql_server_row_count:
#         print('\nRow counts match, proceeding with validation.')
        
#         # Build Snowflake and SQL queries for data retrieval
#         snow_query = f"select * from CTG_DEV.SIGMOID_RAW.{table_name} where DATE({user_column_name}) = '{user_date}'" 
#         sql_query = f"select * from {table_name} where CAST({user_column_name} AS DATE) = '{user_date}'" 

        
#          # Extract table name from the Snowflake table name
#         parts = table_name.split('.')
#         Table_Name = parts[-1]

#         print('Validation Started for Table: ' + Table_Name)

#         # Snowflake table data for the specified date
#         query = snow_query
#         try:
#             test_d = pd.read_sql(query, SnowFlake_connection)
#         except Exception as e:
#             print(f"Error reading data from Snowflake for table {table_name}: {str(e)}")
#             test_d = pd.DataFrame()
#         test_snow = pd.DataFrame(test_d)

#         # SQL Server table data for the specified date
#         SQL_cursor.execute(sql_query)

#         # Fetch all rows from the cursor
#         sql_results = SQL_cursor.fetchall()

#         # Create a DataFrame from the SQL results
#         test_sql = pd.DataFrame(sql_results, columns=[desc[0] for desc in SQL_cursor.description])

#         # Handling boolean value bug of Python SQL connector
#         sql_bool_col = [x for x, y in dict(test_sql.dtypes).items() if y == 'bool']
#         if sql_bool_col:
#             for a in sql_bool_col:
#                 test_sql[a] = test_sql[a].astype(int)


#         # Validation Process for the specified date
#         check = 0
#         if not test_sql.empty:
#             test_sql = test_sql.fillna(0)
#             test_snow = test_snow.fillna(0)
#             test_sql.columns = test_snow.columns  # Set column names for SQL data
#             test_sql = test_sql.astype(test_snow.dtypes)
#             if test_snow.equals(test_sql):
#                 check = 1
#             else:
#                 list_col = test_sql.columns
#                 for a in list_col:
#                     test_sql_temp = test_sql.sort_values([a], ascending=True).reset_index(drop=True)
#                     test_snow_temp = test_snow.sort_values([a], ascending=True).reset_index(drop=True)
#                     if test_snow_temp.equals(test_sql_temp):
#                         check = 1
#                         break
#                     else:
#                         check = 0
#         else:
#             print('----> Table has 0 records' + '\n')
#             check = 2

#         validation_results.append((f'{Table_Name} (Date Validation)', check, test_snow))


#     else:
#         print('Record counts do not match. Skipping validation for Table: ' + table_name)

#     if check == 1:
#         print(f'\nValidation is Done for Table: {table_name}')


# # Close the Snowflake connection
# SnowFlake_connection.close()

# # Check if any validation failed for the specified date
# failed_validations = [result for _, result, _ in validation_results if result == 0]

# if failed_validations:
#     print('\n============= Report =============')
#     print(f'Validation failed for some tables:')
#     for table_name, result, data in validation_results:
#         if result == 0:
#             print(f'\nTable: {table_name}')
#             print('Validation Failed')
#             if not data.empty:
#                 # Find the mismatched rows
#                 mismatched_data = data.merge(test_sql, indicator=True, how='outer').loc[lambda x: x['_merge'] == 'right_only']

#                 # Save the mismatched data to a CSV file
#                 csv_filename = f'Mismatched_data_{table_name}.csv'
#                 mismatched_data.drop('_merge', axis=1, inplace=True)
#                 mismatched_data.to_csv(csv_filename, index=False)
#                 print(f'\nMismatched data saved to: {csv_filename}')






import pymssql
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import json
import random

# Establish SQL Server connections
conn_actual = pymssql.connect("10.158.3.37", "CDC_DMS_User", "xc21kj89ik!", "Development_Market")
SQL_cursor_actual = conn_actual.cursor()

conn_archive = pymssql.connect("10.158.3.37", "CDC_DMS_User", "xc21kj89ik!", "Development_Market_Archive")
SQL_cursor_archive = conn_archive.cursor()

# Establish Snowflake connection
snowflake_params = {
    "account": "cgna-canteen_data",
    "user": "RangaS01",
    "password": "qaws90Ki@#",
    "database": "CTG_DEV",
    "schema": "SIGMOID_RAW",
}

url = URL(**snowflake_params)
engine = create_engine(url)
SnowFlake_connection = engine.connect()
print('Connected to Snowflake')

# Define the list of Snowflake tables to process
with open("table_names.json") as f:
    Snow_table_list = json.load(f)["table_names"]

# Initialize a list to store validation results
validation_results = []

for table_name in Snow_table_list:
    max_fiscal_week_id_query = f"select max(FiscalWeekID) from CTG_DEV.SIGMOID_RAW.{table_name}"
    min_fiscal_week_id_query = f"select min(FiscalWeekID) from CTG_DEV.SIGMOID_RAW.{table_name}"

    max_fiscal_week_id_result = SnowFlake_connection.execute(max_fiscal_week_id_query).fetchone()
    min_fiscal_week_id_result = SnowFlake_connection.execute(min_fiscal_week_id_query).fetchone()

    max_fiscal_week_id = max_fiscal_week_id_result[0]
    min_fiscal_week_id = min_fiscal_week_id_result[0]

    print("Maximum Fiscal Week ID:", max_fiscal_week_id)
    print("Minimum Fiscal Week ID:", min_fiscal_week_id)

    # Generate 5 random fiscal week IDs with non-zero record counts
    random_fiscal_week_ids = []

    while len(random_fiscal_week_ids) < 5:
        fiscal_week_id = random.randint(min_fiscal_week_id, max_fiscal_week_id)

        if fiscal_week_id not in random_fiscal_week_ids:
            # Check if the record count is non-zero for the randomly chosen fiscal week ID
            record_count_actual_query = f"SELECT COUNT(*) FROM {table_name} WHERE FiscalWeekID = '{fiscal_week_id}'"
            SQL_cursor_actual.execute(record_count_actual_query)
            record_count_actual = SQL_cursor_actual.fetchone()[0]
            if record_count_actual > 0:
                random_fiscal_week_ids.append(fiscal_week_id)
            else:
                continue
        else:
            continue

    for fiscal_week_id in random_fiscal_week_ids:
        record_count_actual_query = f"select count(*) from {table_name} where FiscalWeekID = '{fiscal_week_id}'"
        SQL_cursor_actual.execute(record_count_actual_query)
        record_count_actual = SQL_cursor_actual.fetchone()[0]
        print(f"Record count for FiscalWeekID {fiscal_week_id} in actual data for table {table_name}: {record_count_actual}")

        snowflake_record_count_query = f"SELECT COUNT(*) FROM CTG_DEV.SIGMOID_RAW.{table_name} WHERE MODIFIEDDATETIME = '{fiscal_week_id}'"
        snowflake_cursor = SnowFlake_connection.execute(snowflake_record_count_query)
        snowflake_record_count = snowflake_cursor.fetchone()[0]

        if record_count_actual == snowflake_record_count:
            print(f"Record counts for FiscalWeekID {fiscal_week_id} match between actual data in SQL Server and Snowflake for table {table_name}. Proceeding with validation.")
            # Rest of the validation process
            # ...
        else:
            record_count_archival_query = f"select count(*) from {table_name} where FiscalWeekID = '{fiscal_week_id}'"
            SQL_cursor_archive.execute(record_count_archival_query)
            record_count_archival = SQL_cursor_archive.fetchone()[0]
            if record_count_archival == snowflake_record_count:
                print(f"Record counts for FiscalWeekID {fiscal_week_id} match between data in SQL Server and Snowflake for table {table_name}. Proceeding with validation.")
                # Rest of the validation process
                # ...
            elif record_count_archival == 0 and record_count_actual == 0:
                print(f"Skip the records as no data is present in SQL")
                continue
            else:
                print(f"Record counts for FiscalWeekID {fiscal_week_id} do not match between with SQL Server and Snowflake for table {table_name}. Skipping validation.")
                continue

                
        print(f"\nStarting validation for table: {table_name}")

        check = 0

        print('\nRow counts match, proceeding with validation.')

        parts = table_name.split('.')
        Table_Name = parts[-1]

        print('Validation Started for Table: ' + Table_Name)

        query = f"select * from CTG_DEV.SIGMOID_RAW.{table_name} where FiscalWeekID = '{fiscal_week_id}'"
        try:
            test_d = pd.read_sql(query, SnowFlake_connection)
        except Exception as e:
            print(f"Error reading data from Snowflake for table {table_name}: {str(e)}")
            test_d = pd.DataFrame()
        test_snow = pd.DataFrame(test_d)

        if record_count_actual == snowflake_record_count:
            SQL_cursor_actual.execute(f"select * from {table_name} where FiscalWeekID = '{fiscal_week_id}'")
            sql_results = SQL_cursor_actual.fetchall()
        else:
            record_count_archival.execute(f"select * from {table_name} where FiscalWeekID = '{fiscal_week_id}'")
            sql_results = record_count_archival.fetchall()

        test_sql = pd.DataFrame(sql_results, columns=[desc[0] for desc in SQL_cursor_actual.description])

        sql_bool_col = [x for x, y in dict(test_sql.dtypes).items() if y == 'bool']
        if sql_bool_col:
            for a in sql_bool_col:
                test_sql[a] = test_sql[a].astype(int)

        check = 0
        if not test_sql.empty:
            test_sql = test_sql.fillna(0)
            test_snow = test_snow.fillna(0)
            test_sql.columns = test_snow.columns
            test_sql = test_sql.astype(test_snow.dtypes)
            if test_snow.equals(test_sql):
                check = 1
            else:
                list_col = test_sql.columns
                for a in list_col:
                    test_sql_temp = test_sql.sort_values([a], ascending=True).reset_index(drop=True)
                    test_snow_temp = test_snow.sort_values([a], ascending=True).reset_index(drop=True)
                    if test_snow_temp.equals(test_sql_temp):
                        check = 1
                        break
                    else:
                        check = 0
        else:
            print('----> Table has 0 records' + '\n')
            check = 2

        validation_results.append((f'{Table_Name} (Date Validation)', check, test_snow))

    else:
        print('Record counts do not match. Skipping validation for Table: ' + table_name)

    if check == 1:
        print(f'\nValidation is Done for Table: {table_name}')


