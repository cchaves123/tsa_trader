import numpy as np
import time
import uuid

from helpers import get_most_recent_date
import config
from psycopg2 import sql

from api_helpers import get_markets, get_positions, construct_event_ticker
from api_helpers import calc_net_position_ticker, construct_file_name, get_all_orders, call_api
import db_configs
from db_writer import AWS_RDB_CLIENT

    
def get_yes_prob(most_recent_date_string, strike):
    # given a strike, calculate it's fair probability of resulting to yes
    # calulate percentage of simulation rows whose average is greater than the strike

    client = AWS_RDB_CLIENT(db_configs.DB_HOST, db_configs.DB_PORT, db_configs.DB_NAME,
                            db_configs.DB_USER, db_configs.DB_PASSWORD)
    # read simulations from db
    query = sql.SQL(db_configs.QUERY_ALL_SCHEMA).format(table=sql.Identifier(most_recent_date_string),
                                                 schema=sql.Identifier('sims'))
    preds = client.query_sql(query)
    
    # average across rows and return number of averages greater than the strike
    preds_avg = np.array(preds.mean(axis=1))

    return round(100*sum(preds_avg>strike)/len(preds_avg), 0)

def create_orders(most_recent_cutoff):
    # logic to create orders

    yes = {} # will store yes orders
    no = {} # will store no orders
    event_ticker = construct_event_ticker(most_recent_cutoff) # e.g. KXTSAW-25JUL20
    print('EVENT TICKER:', event_ticker)

    # get all the markets in the event
    markets = get_markets(config.KEY_PATH, config.ACCESS_KEY, 'GET', config.BASE_URL,
                          config.MARKETS_PATH, {'event_ticker': event_ticker})
    # get existing positions
    positions = get_positions(config.KEY_PATH, config.ACCESS_KEY, 'GET', config.BASE_URL,
                          config.POSITIONS_PATH, {'event_ticker': event_ticker})

    # place orders for each market
    for market in markets:
        trade_yes = True
        trade_no = True
        dime = True # if True, quote 1 tick above (below) best bid (offer)
        
        # calc net position in this market
        net_position = calc_net_position_ticker(market['ticker'], positions)
        if net_position <= -config.MAX_NET_EXPOSRE_PER_BOOK:
            # if large short position, do not place any more sell orders
            print('RISK LIMIT BREACHED: ', market['ticker'])
            print(net_position, '\n')
            trade_no = False
        if net_position >= config.MAX_NET_EXPOSRE_PER_BOOK:
            # if large long position, do not place any more buy orders
            print('RISK LIMIT BREACHED: ', market['ticker'])
            print(net_position, '\n')
            trade_yes = False
        

        # if the market is trading too close to 0 or 100, do not place any orders
        edge_prob = (market['yes_bid'] + market['yes_ask']) / 2
        if edge_prob > config.YES_BID_UPPER or edge_prob < config.YES_BID_LOWER:
            continue
        if market['yes_bid'] == 0 or market['yes_ask'] == 100:
            continue

        # calculate fair price for the given strike by reading simulation results
        strike = market['floor_strike']
        most_recent_date_string = construct_file_name(most_recent_cutoff)
        theo = get_yes_prob(most_recent_date_string, strike)
        print('\n')
        print(market['ticker'], ' THEO:', theo)
        

        # if best bid and offer are only 1 apart, do not dime; otherwise do
        if market['yes_ask'] - market['yes_bid'] == 1:
            dime = False
        
        if trade_yes:
            # place buy order at minimum of best bid and fair price - edge
            # the bot will never attempt to trade at negative expected value
            # and it will never bid higher than it needs to
            if dime:
                yes_bid = max(min(market['yes_bid'] + 1, theo - config.MIN_EDGE), 0)
            else:
                yes_bid = max(min(market['yes_bid'], theo - config.MIN_EDGE), 0)
            
            # add yes order for that ticker to dict
            yes[market['ticker']] = yes_bid
        
        if trade_no:
            # same logic as yes orders
            # no bids are the same as yes asks
            if dime:
                no_bid = 100 - min(max(market['yes_ask'] - 1, theo + config.MIN_EDGE), 100)
            else:
                no_bid = 100 - min(max(market['yes_ask'], theo + config.MIN_EDGE), 100)

            no[market['ticker']] = no_bid
    

    print('\nORDERS TO PLACE')
    print('YES:', yes)
    print('NO:', no)
    print('\n')
    
    # return dicts of orders to place
    return yes, no


def create_order_yes(ticker, count, price):
    # given a market, number of contracts, and price
    # return dict specifying order for Kalshi API
    
    order_uuid = str(uuid.uuid4())
    params = {
        'action': 'buy',
        'client_order_id': order_uuid,
        'count': count,
        'side': 'yes',
        'ticker': ticker,
        'type': 'limit',
        'yes_price': int(price)
    }
    return params

def create_order_no(ticker, count, price):
    # given a market, number of contracts, and price
    # return dict specifying order for Kalshi API

    order_uuid = str(uuid.uuid4())
    params = {
        'action': 'buy',
        'client_order_id': order_uuid,
        'count': count,
        'side': 'no',
        'ticker': ticker,
        'type': 'limit',
        'no_price': int(price)
    }

    return params

def send_order(ticker, count, side, price):
    # send order via POST to Kalshi API

    if side == 'yes':
        params = create_order_yes(ticker, count, price)
    if side == 'no':
        params = create_order_no(ticker, count, price)

    call_api(config.KEY_PATH, config.ACCESS_KEY, 'POST',
            config.BASE_URL, config.ORDERS_PATH, params)
    
def send_orders(order_dict, side):
    # given yes or no order dict, place each order

    for ticker, price in order_dict.items():
        send_order(ticker, config.UNIT_SIZE_CTS, side, price)
    print(side.upper(), 'ORDERS PLACED SUCCESSFULLY')

def cancel_orders(order_ids):
    # given list of order ids, cancel each one via DELETE

    for order in order_ids:
        params = {}#{'order_id': order}
        cancel_path = config.ORDERS_PATH + '/' + order
        call_api(config.KEY_PATH, config.ACCESS_KEY, 'DELETE',
                 config.BASE_URL, cancel_path, params)

def get_order_ids(event_ticker):
    # for an event, get active order ids across all markets
    # return list of ids

    order_ids = []
    resting_orders = get_all_orders(config.KEY_PATH, config.ACCESS_KEY, 'GET', config.BASE_URL,
                     config.ORDERS_PATH, {'event_ticker': event_ticker, 'status': 'resting'}) # resting = active
    for order in resting_orders:
        order_ids.append(order['order_id'])
    return order_ids


def trader_main():
    # function to place orders

    # cancel all existing orders
    most_recent_date = get_most_recent_date()
    event_ticker = construct_event_ticker(most_recent_date)
    order_ids = get_order_ids(event_ticker)
    cancel_orders(order_ids)

    # create order dicts according to create_orders logic
    yes, no = create_orders(most_recent_date)

    time.sleep(5)

    # place the new orders
    send_orders(yes, 'yes')
    send_orders(no, 'no')
