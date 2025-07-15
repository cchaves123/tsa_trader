from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.exceptions import InvalidSignature
from datetime import datetime
import requests
import json
import numpy as np

from helpers import get_next_sunday, get_previous_sunday
import config


def load_private_key_from_file(file_path):
    # message to API will need to be signed with private key
    # read private key, which is a pem, from path

    with open(file_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,  # or provide a password if your key is encrypted
            backend=default_backend()
        )
    return private_key



def sign_pss_text(private_key: rsa.RSAPrivateKey, text: str) -> str:
    # Before signing, we need to hash our message.
    # The hash is what we actually sign.
    # Convert the text to bytes
    
    message = text.encode('utf-8')

    try:
        signature = private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')
    except InvalidSignature as e:
        raise ValueError("RSA sign PSS failed") from e
    

def call_api(key_file, access_key, method, base_url, path, params={}):
    # generic function to call API
    # key_file - path to private key
    # access_key - Kalshi access key provided on API setup
    # method - GET, POST, DELETE
    # base_url - beginning of Kalshi API url
    # path - end of API url specifying the desired action
    # params - call sometimes pass in a dict of info with a message

    # Get the current time
    current_time = datetime.now()

    # Convert the time to a timestamp (seconds since the epoch)
    timestamp = current_time.timestamp()

    # Convert the timestamp to milliseconds
    current_time_milliseconds = int(timestamp * 1000)
    timestampt_str = str(current_time_milliseconds)

    # Load the RSA private key
    private_key = load_private_key_from_file(key_file)


    # create signature unique to the message
    msg_string = timestampt_str + method + path
    sig = sign_pss_text(private_key, msg_string)

    
    # create headers dict and send message
    headers = {
            'accept': 'application/json',
            'content-type': 'application/json',
            'KALSHI-ACCESS-KEY': access_key,
            'KALSHI-ACCESS-SIGNATURE': sig,
            'KALSHI-ACCESS-TIMESTAMP': timestampt_str,
        }
    if method == 'GET':
        response = requests.get(base_url + path, headers=headers, params=params)
    if method == 'DELETE':
        response = requests.delete(base_url + path, headers=headers, params=params)
    if method == 'POST':
        response = requests.post(base_url + path, json=params, headers=headers)
    
    #print("Status Code:", response.status_code)
    #print("Response Body:", response.text)
    
    return response.text

def get_markets(key_file, access_key, method, base_url, path, params):
    # get all the strikes associated with an event ticker
    return json.loads(call_api(key_file, access_key, method, base_url, path, params))['markets']

def get_positions(key_file, access_key, method, base_url, path, params={}):
    # get all positions in an event
    return json.loads(call_api(key_file, access_key, method, base_url, path, params))['market_positions']

def get_all_orders(key_file, access_key, method, base_url, path, params={}):
    # get all active orders associated with an event
    return json.loads(call_api(key_file, access_key, method, base_url, path, params))['orders']

def construct_event_ticker(most_recent_cutoff):
    # return the current event ticker as a string, e.g KSTSAW-25JUL13
    # most_recent_cutoff - datetime of most recent date with TSA data

    next_sunday = get_next_sunday(most_recent_cutoff) # the current event will close on the next Sunday
    day = str(next_sunday.day)
    if len(day) == 1:
        day = '0' + day
    month = config.MONTH_ABBREVS[next_sunday.month]
    year = str(next_sunday.year)[-2:]

    return config.TSA_TICKER_START + '-' + year + month + day

def construct_file_name(most_recent_cutoff):
    # for the  most recent date with TSA data, construct a datetime

    day = str(most_recent_cutoff.day)
    if len(day) == 1:
        day = '0' + day
    month = config.MONTH_ABBREVS[most_recent_cutoff.month]
    year = str(most_recent_cutoff.year)[-2:]

    return year + month + day

def get_tickers_with_position(positions):
    # return market tickers which contain positions
    # positions - list containing positions in all markets within an event, most of which will be 0

    tickers = []
    for position in positions:
        tickers.append(position['ticker'])
    return tickers


def calc_net_position_ticker(ticker, positions):
    # calculate dollar net exposure for a single market
    
    for position in positions:
        if position['ticker'] == ticker:
            return np.sign(position['position']) * position['market_exposure'] # long/short * exposure
    return 0

def calc_net_position(event_ticker, positions):
    # calc dollar net exposure across all positions

    net_exposure = 0
    for position in positions:
        if position['ticker'].startswith(event_ticker):
            net_exposure += np.sign(position['position']) * position['market_exposure']
    return net_exposure
