import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import db_configs
import requests
from bs4 import BeautifulSoup
import config

class AWS_RDB_CLIENT:
    # object that can query and write to the AWS db
    def __init__(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password

        # connect to db
        self.conn = psycopg2.connect(
            host = db_configs.DB_HOST,
            port = db_configs.DB_PORT,
            dbname = db_configs.DB_NAME,
            user = db_configs.DB_USER,
            password = db_configs.DB_PASSWORD
        )

        self.cursor = self.conn.cursor()

    def write_sql(self, query, data_tuple):
        if data_tuple: # execute commands can contain date to be used in the query
            self.cursor.execute(query, data_tuple)
        else:
            self.cursor.execute(query)

    def query_sql(self, query):
        # return query result as df
        self.cursor.execute(query)
        df = pd.DataFrame(self.cursor.fetchall())
        return df

    def commit(self):
        # commit changes to db
        self.conn.commit()



def scrape_new(url):
    # scrape TSA data from website

    html = requests.get(url).text

    soup = BeautifulSoup(html, 'html5lib')

    # logic to pull text from their html objects
    table = soup.find_all('tbody')[0]
    rows = table.find_all('tr')

    dates = []
    passengers = []
    for row in rows:
        vals = row.find_all('td')
        dates.append(vals[0].get_text())
        passengers.append(float(vals[1].get_text().replace(',', ''))) # convert text containing numbers into floats

    df = pd.DataFrame({'date':dates[::-1], 'passengers':passengers[::-1]})

    return df

def construct_table_name(most_recent_cutoff):
    # construct name of table which will store sims for a certain day
    # most_recent_cutoff - the corresponding table will store sims using data up through this date

    day = str(most_recent_cutoff.day)
    if len(day) == 1:
        day = '0' + day
    month = config.MONTH_ABBREVS[most_recent_cutoff.month]
    year = str(most_recent_cutoff.year)[-2:]

    return year + month + day

def create_preds_table(most_recent_date):
    # create the table that will store the simulation results
    # most_recent_date - same as most_recent_cutoff

    name = construct_table_name(most_recent_date)
    client = AWS_RDB_CLIENT(db_configs.DB_HOST, db_configs.DB_PORT, db_configs.DB_NAME,
                            db_configs.DB_USER, db_configs.DB_PASSWORD)
    
    # execute sql command to create table
    create = sql.SQL(db_configs.CREATE_TABLE).format(table=sql.Identifier(name))
    client.write_sql(create, ())

    try:
        # put this table in the sims schema
        change_schema = sql.SQL(db_configs.CHANGE_SCHEMA).format(table=sql.Identifier(name),
                                                                schema=sql.Identifier('sims'))
        client.write_sql(change_schema, ())
    except:
        client.commit()

    client.commit()



def write_preds(preds, most_recent_date):
    # populate table with simulation results
    # preds - nsims x 7 array containing simulation results

    name = construct_table_name(most_recent_date)

    client = AWS_RDB_CLIENT(db_configs.DB_HOST, db_configs.DB_PORT, db_configs.DB_NAME,
                            db_configs.DB_USER, db_configs.DB_PASSWORD)
    
    # sql insert command
    insert_pred = sql.SQL(db_configs.INSERT_SIM).format(table=sql.Identifier(name),
                                                          schema=sql.Identifier('sims'))
    rows = preds.tolist()

    # psycopg2 function to batch insert new rows
    execute_values(client.cursor, insert_pred, rows)

    client.commit()

def update_db(url):
    # update time series table with new entries

    # scrape df containing all data from the current year
    df = scrape_new(url)

    client = AWS_RDB_CLIENT(db_configs.DB_HOST, db_configs.DB_PORT, db_configs.DB_NAME,
                            db_configs.DB_USER, db_configs.DB_PASSWORD)
    
    # insert each row into time series table
    # the insert query will not insert if a row with that date already exists
    insert_query = sql.SQL(db_configs.INSERT_ROW).format(table=sql.Identifier('all_data'))
    for row in df.itertuples():
        client.write_sql(insert_query, (row.Index, row.date, row.passengers))

    client.commit()
