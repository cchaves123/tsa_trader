# AWS DB endpoint for TSA data and stored simulations - enter your own
DB_HOST = ''

# postgres port
DB_PORT = 5432

# db name found on AWS console - enter your own
DB_NAME = ''

# AWS username - enter your own
DB_USER = ''

# db password set on creation - enter your own
DB_PASSWORD = ''

# insert row command for initially populating time series table
INSERT_ROW = """
    INSERT INTO {table} (id, date, passengers)
    VALUES (%s, %s, %s)
    ON CONFLICT (date) DO NOTHING
"""

# query all TSA data
QUERY_ALL = """
SELECT * FROM {table}
"""

# query all from a sims table
QUERY_ALL_SCHEMA = """
SELECT * FROM {schema}.{table}
"""

# create a sims table with columns for each day
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS {table} (M float, T float, W float, TH float, F float, SA float, SU float)
"""

# insert a sim into sims table
INSERT_SIM = """
    INSERT INTO {schema}.{table} (M, T, W, TH, F, SA, SU)
    VALUES %s
"""

# change sims table to sims schema
CHANGE_SCHEMA = """
ALTER TABLE {table} SET SCHEMA {schema};
"""
