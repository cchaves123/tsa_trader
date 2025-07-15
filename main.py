from helpers import get_most_recent_date, is_uptodate
from api_helpers import construct_event_ticker
from trader import trader_main, get_order_ids, cancel_orders
from db_writer import update_db
from pred_generator import generate_predictions
import config


def main():
    # highest level trading and updating logic

    # boolean determining whether data in AWS db is up to date
    trade = is_uptodate()

    print('UP TO DATE:', trade, '\n')

    if trade or config.BYPASS_UPTODATE:
        # if is up to date or the up to date override is set, place orders
        trader_main()

    else:
        # 
        most_recent_date = get_most_recent_date()
       
        # cancel all existing orders
        event_ticker = construct_event_ticker(most_recent_date)
        order_ids = get_order_ids(event_ticker)
        cancel_orders(order_ids)

        # scrape TSA website for any new data
        update_db(config.SCRAPE_URL)

        # including new data, redetermine whether it is up to date
        trade = is_uptodate()

        if trade:
            # if so, generate simulations containing the update
            generate_predictions(config.NSIMS)
    
         

if __name__ == '__main__':
    main()
