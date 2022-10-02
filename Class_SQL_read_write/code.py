import pandas as pd
import pyodbc

class CLass_SQL:

    def __init__(self
                 , driver_x = "driver_x_default"
                 , server_name = "server_name_default"
                 , db_name = "db_name_default"
                 , user_sql = "user_sql_default"
                 , password_sql = "password_sql_default"
                 ):

        print(f"{'__' * 50} \nCreate SQL connections")

        self.driver_x = driver_x
        self.server_name = server_name
        self.db_name = db_name
        self.user_sql = user_sql
        self.password_sql = password_sql

        try:
            if 'server' in driver_x.lower():
                params_connect = (
                    # "Driver={SQL Server Native Client 11.0};"
                    f"Driver={self.driver_x};\n"  # driver that connects to ODBC
                    f"Server={self.server_name};\n"  # server_name
                    f"Database={self.db_name};\n"  # db_name
                    f"uid={self.user_sql};\n"  # User
                    f"pwd={self.password_sql};\n"  # password
                    f"Trusted_Connection=yes;\n" # it will be based on "automatic" Windows Authentication # When without the SQL Authentication (user + password)
                )


            self.connection = pyodbc.connect( params_connect ) # create teh connection with ODBC python

            print(f"\npyodbc connection completed: {str(self.server_name)}\n")
        except NameError:
            input(f"\npyodbc connection NOT completed: {str(self.server_name)}\n")

        except (Exception, pyodbc.Error) as error:  # To get error that may occur
            input(f"\nError connecting to pyodbc: \n {error}\n")


    def __del__(self):
        print(f"{'__' * 50} \nClosing the connections")

        try:
            self.connection.commit()
            self.connection.close()
            self.engine.dispose()
            print("\nClosing database: Successfully")

        except (Exception, pyodbc.Error) as error:
            print(f"\nError: Closing database: \n {error}\n")

        print(f"{'__' * 50} \n")

        return


    def decorator(func_x):
        from functools import wraps
        @wraps(func_x)
        def log_layout(
                *args #
               , **kwargs #
               ):
            print(f"{'__' * 50} \nRunning Function: '{func_x.__name__}'")
            output = func_x(*args, **kwargs)
            return output
        return log_layout


    @decorator #
    def run_query_sql(self, query):
        print(f"\nRunning SQL query")

        try:
            cursor = self.connection.cursor()  # using the cursor
            cursor.execute(query)  # without parameters

        except (Exception, pyodbc.Error) as error:
            print("\nQuery not completed")
            print(f"\nError: '{error}'")


        sql_data = pd.DataFrame([tuple(row) for row in cursor.fetchall()],
                                columns=[desc[0] for desc in cursor.description])

        cursor.close()

        print('\nQuery is completed\n')

        return sql_data

    @decorator
    def run_script_sql(self, script):
        print(f"\nRunning script")


        try:
            cursor = self.connection.cursor()
            cursor.execute(script)

            cursor.commit()
            cursor.close()

            print('\nQuery is completed')

        except (Exception, pyodbc.Error) as error:
            print("\nScript not completed")
            print(f"\nError: '{error}'")

        return


Db_database = CLass_SQL()


query_x = f"""
select * from Table_Name  column_name = '...'
"""
df = Db_database.run_query_sql(query=query_x, save_x=False)
print(df)

script_update = f"""
Update Table_Name
    Set Column_x = 30
    Where column_name = '...'
"""
Db_database.run_script_sql(script_update)


script_update = f"""
    Delete Table_Name
    Where column_name = '...'
"""
Db_database.run_script_sql(script_update)


script_drop = f"""
    DROP Table Table_Name
    drop table #test_copy
"""
Db_database.run_script_sql(script_drop)