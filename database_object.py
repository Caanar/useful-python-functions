import pandas as pd
import psycopg2
from sshtunnel import SSHTunnelForwarder

class Database:
    """
    Database
    It takes server, port, SQL database, user and password as arguments, and returns an object with the database available for querying.
    ----------
    Attributes
    ----------
    server : str
        Returns the server
    port : int
        Returns the port
    database : str
        Returns the database name
    user : str
        Returns the user
    password : str
        Returns the password
    conn : psychopg2.connection
        Returns the connection
    tables : str
        Returns a list with all the table names on the database
    columns_per_table : str
        Returns a dictionary where the keys are 
        table names and the values are lists containing
        all the column names. For more narrow results 
        use Database.columns_per_table['tablename']
    columns_values_per_table : str
        Returns a dictionary with the key ('tablename', 'column', 'datatype')
        and a pd.Dataframe with a small summary for that column (if available).
        The summary contains the column's distinct values if its text or string,
        or the average, minimum and maximum of the column if its any kind of number type.
    
    -------
    Methods
    -------
    query(str):
        Returns the executed query as a pandas DataFrame object

    """
    def __init__(self, server, port, database, user, password):
        self.server = server
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = psycopg2.connect(
                                dbname =self.database,
                                user = self.user,
                                host = self.server,
                                password = self.password,
                                port = self.port)
        cursor = self.conn.cursor()
        
        #Attribute that holds all the table names in the database
        cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
        self.tables = [t[0] for t in cursor.fetchall()]
        #We intitialize a dict that will hold the columns per table
        tables_and_columns = {}
        for t in self.tables:
            cursor.execute("""select * from {} limit 1""".format(t))
            tables_and_columns[t] = [desc[0] for desc in cursor.description]
        self.columns_per_table = tables_and_columns
        tables_columns_types = pd.read_sql("""SELECT table_name, column_name, data_type FROM information_schema.columns 
                                                   WHERE table_schema = 'public' order by table_name, column_name, data_type""", self.conn)
        col_values = {}
        for i, r in tables_columns_types.iterrows():
            if r['data_type'] in ['character varying','text']:
                try:
                    col_values[(r['table_name'],r['column_name'],r['data_type'])] = pd.read_sql("""select distinct {} 
                                                                                            from {}""".format(r['column_name'],
                                                                                            r['table_name']), self.conn)
                except:
                    col_values[(r['table_name'],r['column_name'],r['data_type'])] = 'No Summary Available'
            elif r['data_type'] in ['bigint', 'double precision', 'integer', 'numeric']:
                try:
                    col_values[(r['table_name'],r['column_name'],r['data_type'])] = pd.read_sql("""select avg({}),
                                                                                            min({}), 
                                                                                            max({}) from {}""".format(r['column_name'],
                                                                                            r['column_name'],
                                                                                            r['column_name'],
                                                                                            r['table_name']), self.conn)
                except:
                    col_values[(r['table_name'],r['column_name'],r['data_type'])] = 'No Summary Available'
            else: 
                col_values[(r['table_name'],r['column_name'],r['data_type'])] = 'No Summary Available'
                
        self.column_values_per_table = col_values 
        
    # Method that allows to execute a query and returns a dataframe
    def query(self, query):
        return pd.read_sql(query, self.conn)

# Examples of use:
# Initiate a Database object
db = Database('db.com', 5432, 'uat_db', 'admin', 'database_pass')

# Access some attributes sucha as tables, columns per table or column values per table
db.tables
db.columns_per_table
db.column_values_per_table

# SQL queries:
df = db.query("""SELECT co.id, co."name" as "Country Name",
             sum(me.price*sa.units) as "Country Income (sales)"
             FROM Country co
             where sa.sale_timestamp >= date_trunc('month', current_date - interval '1' month)
             group by co.id""")
