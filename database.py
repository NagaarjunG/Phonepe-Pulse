from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float,String, VARCHAR,BIGINT,DECIMAL,DOUBLE, text,UniqueConstraint
from sqlalchemy.exc import IntegrityError
import mysql.connector






class DB_Mgmt:
    def __init__(self):
        self.db_host = 'localhost'
        self.db_user = 'root'
        self.db_password = 'microbio_root'
        self.db_name = 'PhonepeNew'

        self.db_connection = f"mysql+mysqlconnector://{self.db_user}:{self.db_password}@{self.db_host}"
        self.database_name = "PhonepeNew"
        self.engine = create_engine(self.db_connection)

    def create_database(self):
        try:
            # Establish a connection to the MySQL server
            connection = self.engine.connect()

            # Create the database if it doesn't exist
            create_db_statement = text(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
            connection.execute(create_db_statement)

            # Close the connection explicitly
            connection.close()
        except Exception as e:
            print(f"An error occurred while creating the database: {e}")
            raise

    def df_to_sql(self, df_details, table_name):
        try:
            # Establish a connection to the MySQL server
            connection = self.engine.connect()

            # Use the database
            use_db_statement = text(f"USE {self.database_name}")
            connection.execute(use_db_statement)

            # Push the dataframe to sql
            df_details.to_sql(table_name, con=self.engine, if_exists='append', index=False, schema=self.database_name)

            # Close the connection explicitly
            connection.close()

        except IntegrityError as e:
           
            pass
        except Exception as e:
            print(f"An error occurred while creating the database: {e}")
            raise
    



    def table_creation(self):
        try:
            # Establish a connection to the MySQL server
            connection = self.engine.connect()

            # Use the database
            use_db_statement = text(f"USE {self.database_name}")
            connection.execute(use_db_statement)

            # Define metadata
            metadata = MetaData()

            # Define tables
            agg_transactions_table = Table(
                'agg_transactions', metadata,
                Column('States', VARCHAR(100)),
                Column('Years', VARCHAR(5)),
                Column('Quarter', VARCHAR(2)),
                Column('Transaction_type', VARCHAR(100)),
                Column('Transaction_count', BIGINT),
                Column('Transaction_amount', DECIMAL(50,2)),
                UniqueConstraint('States', 'Years', 'Quarter', 'Transaction_type',name='unique_agg_transactions_constraint'),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            agg_transactions_table.create(self.engine, checkfirst=True)

            agg_users_table = Table(
                'agg_users', metadata,
                Column('States', VARCHAR(100)),
                Column('Years', VARCHAR(5)),
                Column('Quarter', VARCHAR(2)),
                Column('Brands', VARCHAR(100)),
                Column('User_Count', Integer),
                Column('Percentage', DOUBLE),
                UniqueConstraint('States', 'Years', 'Quarter', 'Brands', name='unique_agg_users_constraint'),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            agg_users_table.create(self.engine, checkfirst=True)

            map_transactions_table = Table(
                'map_transactions', metadata,
                Column('States', VARCHAR(100)),
                Column('Years', VARCHAR(5)),
                Column('Quarter', VARCHAR(2)),
                Column('Districts', VARCHAR(100)),
                Column('Transaction_count', BIGINT),
                Column('Transaction_amount', DECIMAL(50,2)),
                UniqueConstraint('States', 'Years', 'Quarter', 'Districts', name='unique_map_transactions_constraint'),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            map_transactions_table.create(self.engine, checkfirst=True)

            map_users_table = Table(
                'map_users', metadata,
                Column('States', VARCHAR(100)),
                Column('Years', VARCHAR(5)),
                Column('Quarter', VARCHAR(2)),
                Column('Districts', VARCHAR(100)),
                Column('RegisteredUsers', Integer),
                Column('AppOpens', Integer),
                UniqueConstraint('States', 'Years', 'Quarter', 'Districts', name='unique_map_users_constraint'),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            map_users_table.create(self.engine, checkfirst=True)

            top_transactions_table = Table(
                'top_transactions', metadata,
                Column('States', VARCHAR(100)),
                Column('Years', VARCHAR(5)),
                Column('Quarter', VARCHAR(2)),
                Column('Pincodes', VARCHAR(8)),
                Column('Transaction_count', BIGINT),
                Column('Transaction_amount', DECIMAL(50,2)),
                UniqueConstraint('States', 'Years', 'Quarter', 'Pincodes', name='unique_top_transactions_constraint'),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            top_transactions_table.create(self.engine, checkfirst=True)

            top_users_table = Table(
                'top_users', metadata,
                Column('States', VARCHAR(100)),
                Column('Years', VARCHAR(5)),
                Column('Quarter', VARCHAR(2)),
                Column('Pincodes', VARCHAR(8)),
                Column('RegisteredUsers', Integer),
                UniqueConstraint('States', 'Years', 'Quarter', 'Pincodes', name='unique_top_users_constraint'),
                schema=self.database_name
            )

            # Create the table if it doesn't exist
            top_users_table.create(self.engine, checkfirst=True)

            # Close the connection explicitly
            connection.close()

        except IntegrityError as ie:
            print(f"An integrity error occurred while creating the tables: {ie}")
            raise

        except Exception as e:
            print(f"An error occurred while creating the database: {e}")
            raise

    def Query_Output(self, Chnl_Chk_Query):
        try:
            # Establish a connection to the MySQL server
            connection = self.engine.connect()

            # Use the database
            use_db_statement = text(f"USE {self.database_name}")
            connection.execute(use_db_statement)

            select_statement = text(Chnl_Chk_Query)
            result = connection.execute(select_statement)
            channel_result = result.fetchall()

            if channel_result:
                return channel_result
            else:
                return None
            connection.close()

        except Exception as e:
            print(f"An error occurred while creating the database: {e}")
            raise
