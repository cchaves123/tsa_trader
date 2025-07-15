from datetime import datetime, timedelta, time
import pandas as pd
from db_writer import AWS_RDB_CLIENT
import db_configs
from psycopg2 import sql


def to_datetime(date):
    # convert date object to datetime
    return datetime.combine(date, time(0, 0, 0))

def get_next_sunday(most_recent_cutoff):
    # given the most recent date with TSA data, get the next Sunday as a datetime
    
    days_ahead = 6 - most_recent_cutoff.weekday()  # weekday(): Monday is 0, Sunday is 6
    if days_ahead <= 0:
        days_ahead += 7
    next_sunday = most_recent_cutoff + timedelta(days=days_ahead)
    return next_sunday

def get_previous_sunday(most_recent_cutoff):
    # given the most recent date with TSA data, get the previous Sunday

    next_sunday = get_next_sunday(most_recent_cutoff)
    return next_sunday - timedelta(days=7)


def get_most_recent_date():
    # return the most recent date with TSA data
    # on Monday thru Thursday, this will usually be today - 1 day
    # on Friday thru Sunday, this will usually be the previous Thursday

    all_data = get_all_data()
    return all_data.iloc[-1, 0]

def is_uptodate():
    # logic to determine whether the data in the AWS db is up to date

    most_recent_datetime = get_most_recent_date() # most recent date with TSA data
    now_time = datetime.now() # current date

    # TSA data for the previous day is usually updated oon the website each weekday around 9 AM
    # Monday's update contains data from Friday, Saturday, and Sunday
    # the system begins looking for the update at 7 AM to be safe


    if now_time.weekday() <= 4:
        if now_time.hour < 7:
            # if Monday thru Friday and before 7 AM, the data is up to date if it has data from 2 days ago
            # suppose it is 3 AM on Thursday, Wednesday's data will not be posted yet
            # so the db is up to date as long as it contains Tuesday
            up_to_date = now_time - timedelta(days=2)          
        elif now_time.hour >= 7:
            # if Monday thru Friday and after 7 AM, the data is up to date only if it has the previous day
            up_to_date = now_time - timedelta(days=1)
    
    else:
         # if Sat or Sun, the data is up to date if it contains the previous Thursday
         shift = now_time.weekday() - 3
         up_to_date = now_time - timedelta(days=shift)

    if up_to_date.date() == most_recent_datetime.date():
        # check whether the up to date criterion is met for the most recent date
        return True
    else:
        return False


def df_for_prophet(cutoff):
    # construct df to be fit by Prophet
    # cutoff - only fit data after this date

    df_to_fit = get_all_data()
    df_to_fit = df_to_fit[df_to_fit.date >= cutoff]
    df_to_fit.rename(columns={'date':'ds', 'passengers':'y'}, inplace=True)
    return df_to_fit


def get_all_data():
    # get whole time series dataframe from AWS db

    client = AWS_RDB_CLIENT(db_configs.DB_HOST, db_configs.DB_PORT, db_configs.DB_NAME,
                            db_configs.DB_USER, db_configs.DB_PASSWORD)
    
    # get all data as df
    query = sql.SQL(db_configs.QUERY_ALL).format(table=sql.Identifier('all_data'))
    all_data = client.query_sql(query)
    
    # get correct columns
    all_data.drop(columns=[0], axis=1, inplace=True)
    all_data.rename(columns={1:'date', 2:'passengers'}, inplace=True)
    
    # sort by date and convert date to datetime
    all_data = all_data.sort_values('date').reset_index(drop=True)
    all_data['date'] = pd.to_datetime(all_data['date'])
    
    return all_data
