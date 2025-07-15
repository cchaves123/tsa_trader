import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from prophet import Prophet
from statsmodels.tsa.arima.model import ARIMA
from tqdm import tqdm

from helpers import get_next_sunday, get_previous_sunday, get_all_data, df_for_prophet, to_datetime
import config

from db_writer import create_preds_table, write_preds

CUTOFF = datetime(2022, 1, 1)
PATH = 'all_data_raw.csv'


def df_for_arma(forecast, all_data, most_recent_date):
    # create df containing Prophet prediction errors that can be used to fit the ARMA model

    # merge df containing Prophet predictions with df containing actual values
    arma_df = pd.merge(left=forecast[forecast.ds<=most_recent_date][['ds','yhat', 'yhat_lower', 'yhat_upper']],
                   right=all_data[['date', 'passengers']],
                   how='left', left_on='ds', right_on='date')
    
    # configure df for arma model
    arma_df['error'] = arma_df['yhat'] - arma_df['passengers']
    arma_df = arma_df[['ds', 'error']]
    arma_df.set_index('ds', inplace=True)
    return arma_df

def fit_arma(arma_df, p, d, q):
    # return fitted ARMA
    return ARIMA(arma_df['error'], order=(p, d, q), freq='D').fit()

def fit_prophet(df_to_fit):
    # fit Prophet model on df

    prophet_model = Prophet(interval_width=0.95, changepoint_prior_scale=0.05) # specify trend flexibility
    prophet_model.add_seasonality(name='monthly', period=30.5, fourier_order=5) # add monthly seasonality
    prophet_model.add_country_holidays(country_name='US') # add holiday effects
    prophet_model.fit(df_to_fit[['ds', 'y']])
    return prophet_model

def simulate(arma_model, prophet_preds, nsims, anchor, days_left):
    # generate simulated paths that will be used to calculate fair values
    # arma_model - fitted arma model used to generate paths
    # prophet_preds - Prophet point predictions for the remainder of the week
    # nsims - how many simulations to generate
    # anchor - first simulation day
    # days_left - how many days to simulate, i.e. if the most recent date is a Thursday, days left is 3

    print('FIRST SIM DAY:', anchor)
    sims = np.array([np.zeros(days_left)])
    for i in range(nsims):
        # simulate path
        sims = np.vstack((sims, arma_model.simulate(nsimulations=days_left, anchor=anchor)))
    sims = sims[1:]
    
    # combine arma sims with prophet preds
    preds = prophet_preds - sims

    return preds # array of size nsims x days_left

def get_previous_results(all_data, most_recent_cutoff):
    # get actual passenger values already recorded this week
    # e.g. if most recent cutoff is a Wednesday, return the values from Mon, Tue, and Wed

    previous_sunday = get_previous_sunday(most_recent_cutoff)
    previous_df = all_data[(all_data.date<=most_recent_cutoff)&
                           (all_data.date>previous_sunday)] # actual passenger values after last Sunday
    return np.array(previous_df.passengers)

def append_previous_results(nsims, preds, previous_results):
    # append each simulation row with the week's actual passenger values
    # this creates an array of size nsims x 7
    # these rows can then be averaged to simulate a draw from the week's average distribution

    prev_extended = np.tile(previous_results, (nsims,1))
    assert np.hstack((prev_extended, preds)).shape[1] == 7
    return np.hstack((prev_extended, preds))


def save_preds(preds, most_recent_cutoff):
    # function to save simulations in a csv (not used)
    day = str(most_recent_cutoff.day)
    month = config.MONTH_ABBREVS[most_recent_cutoff.month]
    year = str(most_recent_cutoff.year)[-2:]

    path = 'sims/' + year + month + day

    np.save(path, preds)


def generate_predictions(nsims):
    # big function to generate and store simulation results in the AWS db

    # get df for Prophet and big time series df
    df_to_fit = df_for_prophet(CUTOFF)
    all_data = get_all_data()

    most_recent_date = all_data.iloc[-1, 0]
    most_recent_datetime = to_datetime(most_recent_date)
    
    print('MOST RECENT: ', most_recent_datetime)
    
    # next sunday datetime
    next_sunday = get_next_sunday(most_recent_datetime)
    print('NEXT SUNDAY: ', next_sunday)

    # days to forecast
    days_left = (next_sunday - most_recent_datetime).days
    print('DAYS TO FORECAST: ', days_left)

    # fit prophet and forecast both over training set and remainder of week
    prophet_model = fit_prophet(df_to_fit)
    future = prophet_model.make_future_dataframe(periods=days_left)
    forecast = prophet_model.predict(future)
    print('prophet forecasted')

    
    # fit arma model on prophet errors over training set
    arma_df = df_for_arma(forecast, all_data, most_recent_datetime)
    arma_model = fit_arma(arma_df, 14, 0, 14)
    print('arma fit')

    
    # simulate outcomes over the remainder of the week
    # combine with prophet predictions
    prophet_preds = np.array(forecast.tail(days_left)['yhat'])
    anchor = most_recent_datetime + timedelta(days=1)
    preds = simulate(arma_model, prophet_preds, nsims, anchor, days_left)

    # append this week's recorded values to simulated results
    previous_results = get_previous_results(all_data, most_recent_datetime)
    extended_preds = append_previous_results(nsims, preds, previous_results)

    print(extended_preds)

    #save_preds(np.mean(extended_preds, axis=1), most_recent_datetime)

    # save the simulation results (size nsims x 7) in db in sims schema
    create_preds_table(most_recent_datetime)
    write_preds(extended_preds, most_recent_datetime)

    return np.mean(extended_preds, axis=1) # nsims X 1 array containing weekly averages

#preds = generate_predictions(100000)
#print('forecast: ', np.percentile(preds, 50))
#print(sum(preds>2600000)/100000)
#print(sum(preds>2650000)/100000)
#print(sum(preds>2700000)/100000)
#print(sum(preds>2750000)/100000)
#print(sum(preds>2800000)/100000)
