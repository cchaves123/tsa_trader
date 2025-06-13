import pandas as pd
import numpy as np
from datetime import datetime
from prophet import Prophet
from statsmodels.tsa.arima.model import ARIMA
from tqdm import tqdm

from helpers import get_next_sunday, get_previous_sunday

CUTOFF = datetime(2022, 1, 1)
PATH = 'all_data_raw.csv'

def df_for_prophet(path=PATH):
    df_to_fit = pd.read_csv(path, usecols=['date', 'passengers'])
    df_to_fit['date'] = df_to_fit['date'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y'))
    df_to_fit.sort_values('date').reset_index(drop=True, inplace=True)
    df_to_fit = df_to_fit[df_to_fit.date >= CUTOFF]
    df_to_fit.rename(columns={'date':'ds', 'passengers':'y'}, inplace=True)
    return df_to_fit

def get_all_data(path=PATH):
    all_data = pd.read_csv(path, usecols=['date', 'passengers'])
    all_data['date'] = all_data['date'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y'))
    return all_data.sort_values('date').reset_index(drop=True)

def df_for_arma(forecast, all_data, most_recent_date):
    arma_df = pd.merge(left=forecast[forecast.ds<=most_recent_date][['ds','yhat', 'yhat_lower', 'yhat_upper']],
                   right=all_data[['date', 'passengers']],
                   how='left', left_on='ds', right_on='date')
    arma_df['error'] = arma_df['yhat'] - arma_df['passengers']
    arma_df = arma_df[['ds', 'error']]
    arma_df.set_index('ds', inplace=True)
    return arma_df

def fit_arma(arma_df, p, d, q):
    return ARIMA(arma_df['error'], order=(p, d, q), freq='D').fit()

def fit_prophet(df_to_fit):
    prophet_model = Prophet(interval_width=0.95, changepoint_prior_scale=0.05)
    prophet_model.add_country_holidays(country_name='US')
    prophet_model.fit(df_to_fit[['ds', 'y']])
    return prophet_model

def simulate(arma_model, prophet_preds, nsims, most_recent_cutoff, days_left):
    sims = np.array([np.zeros(days_left)])
    for i in tqdm(range(nsims)):
        sims = np.vstack((sims, arma_model.simulate(nsimulations=days_left, anchor=most_recent_cutoff)))
    sims = sims[1:]
    preds = prophet_preds - sims

    return preds

def get_previous_results(all_data, most_recent_cutoff):
    previous_sunday = get_previous_sunday(most_recent_cutoff)
    previous_df = all_data[(all_data.date<=most_recent_cutoff)&
                           (all_data.date>previous_sunday)]
    return np.array(previous_df.passengers)

def append_previous_results(nsims, preds, previous_results):
    prev_extended = np.tile(previous_results, (nsims,1))
    assert np.hstack((prev_extended, preds)).shape[1] == 7
    return np.hstack((prev_extended, preds))

#get most recent date in all_data
#get date of next sunday
#fit prophet model on entire df
#forecast necessary amount of days into future
#create error series
#fit ar(28) model to errors
#simulate error series n times
#add to prophet preds and average
#return distribution of averages
def generate_predictions(nsims):
    df_to_fit = df_for_prophet()
    all_data = get_all_data()

    most_recent_date = all_data.iloc[-1, 0]
    print('MOST RECENT: ', most_recent_date)
    next_sunday = get_next_sunday(most_recent_date)
    print('NEXT SUNDAY: ', next_sunday)

    days_left = (next_sunday - most_recent_date).days
    print('DAYS TO FORECAST: ', days_left)

    prophet_model = fit_prophet(df_to_fit)

    future = prophet_model.make_future_dataframe(periods=days_left)
    forecast = prophet_model.predict(future)
    print('forecasted')

    arma_df = df_for_arma(forecast, all_data, most_recent_date)

    arma_model = fit_arma(arma_df, 28, 0, 0)
    print('arma fit')


    prophet_preds = np.array(forecast.tail(days_left)['yhat'])
    
    preds = simulate(arma_model, prophet_preds, nsims, most_recent_date, days_left)

    previous_results = get_previous_results(all_data, most_recent_date)

    extended_preds = append_previous_results(nsims, preds, previous_results)

    print(extended_preds)
    return np.mean(extended_preds, axis=1)

preds = generate_predictions(100000)
print('forecast: ', np.percentile(preds, 50))
print(sum(preds>2600000)/100000)
print(sum(preds>2650000)/100000)
print(sum(preds>2700000)/100000)
print(sum(preds>2750000)/100000)