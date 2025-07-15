# path to RSA private key
KEY_PATH = 'kalshi_key.txt'

# API access key - enter your own
ACCESS_KEY = ''

# beginning of Kalshi API endpoint
BASE_URL = 'https://api.elections.kalshi.com'

# endpoint for positions info
POSITIONS_PATH = '/trade-api/v2/portfolio/positions'

# endpoint for markets info
MARKETS_PATH = '/trade-api/v2/markets'

# endpoint for orders info
ORDERS_PATH = '/trade-api/v2/portfolio/orders'

# how all event tickers in the TSA check-in series begin
TSA_TICKER_START = 'KXTSAW'

# dict mapping month to Kalshi event ticker month abbrevs
MONTH_ABBREVS = {
    1 : 'JAN',
    2 : 'FEB', 
    3 : 'MAR', 
    4 : 'APR',
    5 : 'MAY', 
    6 : 'JUN', 
    7 : 'JUL', 
    8 : 'AUG', 
    9 : 'SEP', 
    10 : 'OCT', 
    11 : 'NOV',
    12 : 'DEC'
}

# if True, trade even if internal logic determines the data is not up to date
BYPASS_UPTODATE = False

# how many sims used to generate fair values
NSIMS = 100000

# max net exposure across all markets
MAX_NET_EXPOSRE = 20000

# max net exposure for a single market
MAX_NET_EXPOSRE_PER_BOOK = 5000

# size of orders to place in number of contracts
UNIT_SIZE_CTS = 100

# if yes bid is below this, do not trade this market
YES_BID_LOWER = 15

# if yes bid is above this, do not trade this market
YES_BID_UPPER = 85

# buy (sell) orders must be quoted at least 6 ticks below (above) the calculated fair value
MIN_EDGE = 6

# beginning at this time, do not place orders until the updated data is posted
NO_TRADE_START = (7, 0)

# if the updated data is not posted by this time, continue trading as normal
NO_TRADE_END = (10, 30)

# TSA passenger data web page
SCRAPE_URL = 'https://www.tsa.gov/travel/passenger-volumes/'
