import sys, os, traceback
import time
import datetime
import dateutil.parser as dp
import threading
import logging
import math
import cbpro as cb
from pytz import timezone  
from collections import deque, Counter, OrderedDict
import numpy as np
from numpy import cumsum, log, polyfit, sqrt, std, subtract
from numpy.random import randn
from scipy import stats
from random import randint
from tqdm import tqdm
#import mysql.connector
from pyfiglet import Figlet



east_coast = timezone('US/Eastern')


def main(api_key, secret,passphrase, minimum_window, trade_amount_btc):
    global products

    f = Figlet(font='slant')
    print f.renderText('Alice'), "Begin Market Making..."
    trade_amount_btc = float(trade_amount_btc)
    minimum_window = int(minimum_window)

    cbpro = cb.AuthenticatedClient(api_key, secret, passphrase)
    products = cbpro.get_products()

    patterns = [
                ["ETH-BTC"]
                # ["XRP-BTC"],
                # ["LTC-BTC"]  
                ]                                   


    for pattern in patterns:
        trade_thread = threading.Thread(target=strategy, args=(cbpro, minimum_window, pattern,trade_amount_btc))
        trade_thread.start()



###################################################################

def strategy(cbpro, minimum_window, pattern,trade_amount_btc):   
    global products

    # mydb = mysql.connector.connect(
    # host = "localhost",
    # user = "root",
    # passwd = "katie321654987",
    # database = "researchDB"
    # )
    # mc = mydb.cursor()
    # sqlFormula = "INSERT INTO macdz_hist (timestamp,mid_price,slow_z,fast_z,coin) VALUES (%s,%s,%s,%s,%s)"

    pair_one = pattern[0]
    ticker_volume = cbpro.get_product_ticker(product_id=pair_one)['volume']
    ###### ENTER PARAMETERS #########
    volume_limit = 50
    window = minimum_window * 34560 # <----------- past 6 hrs 8640 34560 = 24hr
    price_window = deque([],window)
    flow_hist = []
    #alloc_pct = 0.0033
    min_trade = get_min_trade(products,pair_one)
    size_round = num_after_point(float(ticker_volume))
    trade_size = trade_amount_btc
    signal = 2
    ################################
    
    while 1:
        try:
            #db_inserts = []
            
            # wait for enough data before trading
            if len(price_window) < window:
                for i in tqdm(range(0,window),desc=pair_one):
                    orderbook = cbpro.get_product_order_book(pair_one,level=2)

                    ## get latest best bids and asks
                    pairOneAsk = float(orderbook['asks'][0][0])
                    pairOneBid = float(orderbook['bids'][0][0])
                    mid_price =  (pairOneAsk + pairOneBid)/2 
                    price_window.append(mid_price)
                    time.sleep(2.5)
                continue

            ## 24HR VOLUME GATE: 
            vol_24      = cbpro.get_product_24hr_stats(pair_one)
            if 'message' in vol_24:
                print vol_24['message'], pair_one
                time.sleep(1)
                continue
            base_volume = mid_price * float(vol_24['volume'])         
            time.sleep(0.5)
            ############################

            orderbook = cbpro.get_product_order_book(pair_one,level=2)
            if 'message' in orderbook:
                print orderbook['message'], pair_one
                time.sleep(1)
                continue
            bid_top = orderbook['bids']
            ask_top = orderbook['asks']
            

            ## get latest best bids and asks
            pairOneAsk = float(orderbook['asks'][0][0])
            pairOneBid = float(orderbook['bids'][0][0])
            mid_price =  (pairOneAsk + pairOneBid)/2 
            price_window.append(mid_price)  
            if num_after_point(orderbook['bids'][0][0]) > num_after_point(orderbook['asks'][0][0]):
                price_round = num_after_point(orderbook['bids'][0][0])
            else:
                price_round = num_after_point(orderbook['asks'][0][0])
            
            time.sleep(1)
            ###################################################
            
            # rest API calls
            sell_trade   = pair_one.split('-')[0]
            base_trade   = pair_one.split('-')[1]
            wallet       = cbpro.get_accounts()
            minor_balance    = get_minor(sell_trade,wallet)
            base_balance     = get_base(base_trade,wallet)

            time.sleep(0.5)
            open_balance   = list(cbpro.get_orders(product_id=pair_one))
            buy_order_num  = get_buy_order(open_balance)
            sell_order_num = get_sell_order(open_balance)
        

            ## calculate trade size
            #total_acc_value = get_total_balance(pattern,wallet,sell_trade) + base_balance
            #trade_size      = total_acc_value * alloc_pct 
            

            ### clean up
            if 0 < minor_balance < min_trade and len(sell_order_num) == 0: 
                cbpro.place_market_order(product_id=pair_one, side='buy', size=min_trade)
                time.sleep(3)
                print "fixing partial sell", pair_one
                wallet = cbpro.get_accounts()
                minor_balance = get_minor(sell_trade,wallet)
                print "fixing partial sell -- > sold balance", pair_one
                cbpro.place_market_order(product_id=pair_one, side='sell', size=minor_balance)
                time.sleep(1)
                continue
            if len(buy_order_num) > 1:
                cbpro.cancel_order(buy_order_num[0])
            if len(sell_order_num) > 1:
                cbpro.cancel_order(sell_order_num[0])
 
            ## calculate orderbook depth
            est_volumne = (base_volume/24) * 0.25 # 15mins of trade volume
            bid_depth = depth(bid_top,est_volumne)
            ask_depth = depth(ask_top,est_volumne)

            ############# TRADING LOGIC  ###################

            price_list = np.array(list(price_window)) 
            hurst_exp = round(float(hurst(price_list)),2)
            z_score = round(float(list(stats.zscore(price_list))[-1]),2)

            # calcuate vwaps
            orderbook_bid = orderbook['bids'][:bid_depth]  
            vwap_bid = sum([float(x[0])*float(x[1]) for x in orderbook_bid])/sum([float(x[1]) for x in orderbook_bid])
            buy_rate = round(vwap_bid,price_round)
            buy_size = round(trade_size/buy_rate,size_round-1)

            orderbook_ask = orderbook['asks'][:ask_depth]  
            vwap_ask = sum([float(x[0])*float(x[1]) for x in orderbook_ask])/sum([float(x[1]) for x in orderbook_ask])
            sell_rate = round(vwap_ask,price_round)
            sell_size = round(trade_size/sell_rate,size_round-1)

            # order flow logic
            # time.sleep(1)
            # flow_imbalance = 0
            # signal = 1.5
            # if z_score < -signal and hurst_exp < 0.5:
            #     order_history = list(cbpro.get_product_trades(product_id=pair_one)) # cbpro only returns last 100 trades
            #     flow_imbalance = scaled_flow_imbal(order_history)            
            #     if flow_imbalance < 0:
            #         signal = 2.5 


            #### BUY LOGIC
            if buy_rate == get_buy_price(open_balance) or buy_size < min_trade:
                time.sleep(6)
                continue
            if hurst_exp < 0.5 and z_score < -signal and base_volume >= volume_limit: 
                if len(sell_order_num) == 1:
                    cbpro.cancel_order(sell_order_num[0])
                if len(buy_order_num) == 0:
                    cbpro.place_limit_order(product_id=pair_one,post_only = 'True', side='buy', price=buy_rate, size=buy_size)
                    print "Buy -->", pair_one,bid_depth,z_score,datetime.datetime.now(east_coast).strftime("%m-%d-%Y %H:%M:%S")   
                elif len(buy_order_num) == 1:
                    cbpro.place_limit_order(product_id=pair_one,post_only = 'True', side='buy', price=buy_rate, size=buy_size)
                    cbpro.cancel_order(buy_order_num[0])



            #### SELL LOGIC
            if sell_rate == get_sell_price(open_balance):
                time.sleep(6)
                continue
            if hurst_exp < 0.5 and z_score > signal:
                if len(buy_order_num) == 1:
                    cbpro.cancel_order(buy_order_num[0])
                if 0 < minor_balance <= sell_size and len(sell_order_num) == 0: 
                    cbpro.place_limit_order(product_id=pair_one,post_only = 'True', side='sell', price=sell_rate, size=minor_balance)
                    print "Selling Remaining Balance", pair_one
                elif minor_balance > 0 and len(sell_order_num) == 0:
                    cbpro.place_limit_order(product_id=pair_one,post_only = 'True', side='sell', price=sell_rate, size=sell_size)
                    print "Sell -->", pair_one,ask_depth,z_score,datetime.datetime.now(east_coast).strftime("%m-%d-%Y %H:%M:%S")   
                elif len(sell_order_num) == 1:
                    cbpro.place_limit_order(product_id=pair_one,post_only = 'True', side='sell', price=sell_rate, size=sell_size)
                    cbpro.cancel_order(sell_order_num[0])

                    

            ############ DB INSERTS ######################
            current_unix  = time.time() 

            # db_inserts.append(current_unix)
            # db_inserts.append(mid_price)
            # db_inserts.append(slow_z)
            # db_inserts.append(fast_z)
            # db_inserts.append(pair_one)

            # mc.execute(sqlFormula,db_inserts)
            # mydb.commit()

            ########################################

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            tb = traceback.extract_tb(exc_tb)[-1]
            print(exc_type, tb[2], tb[1],pair_one)
            pass
        
            
        #time.sleep(1.5)
        time.sleep(randint(5,7))


        


############ ORDER HISTORY FUNCTIONS ##############

def num_after_point(x):
    s = str(x)
    if not '.' in s:
        return 0
    return len(s) - s.index('.') - 1


def get_min_trade(products,pair_one):
    for i in products:
        if i['id'] == pair_one:
            return float(i['base_min_size'])

def get_buy_price(orders):
    for i in orders:
        if i['side'] == 'buy':
            return float(i['price'])
           
def get_sell_price(orders):
    for i in orders:
        if i['side'] == 'sell':
            return float(i['price'])           

def get_buy_order(orders):
    buy_id = []
    for i in orders:
        if i['side'] == 'buy':
            buy_id.append(i['id'])
    return buy_id

def get_sell_order(orders):
    sell_id = []
    for i in orders:
        if i['side'] == 'sell':
            sell_id.append(i['id'])
    return sell_id
  

def get_base(coin,wallet):
    for i in wallet:
        if i['currency'] == coin:
            base = i['balance']
    return float(base)

def get_minor(coin,wallet):
    for i in wallet:
        if i['currency'] == coin:
            available = i['available']
    return float(available)

# def get_total_balance(coins,wallet,sell_trade):
#     btc_total = []

#     for p in patterns:
#         for i in wallet:
#             if i['currency'] == sell_trade:
#                 bal = float(i['balance'])*mid_price
#                 btc_total.append(bal)

#     return sum(btc_total)

def build_flow_hist(flow):       
    curr_hist = []
 
    for t in flow:
        parsed_t = dp.parse(t['time'])
        unix = float(parsed_t.strftime('%s')) 
        size = t['size']
        price = t['price']
        side = t['side']
        trade_id = t['trade_id']
        full = [unix,size,price,side,trade_id]
        curr_hist.append(full)
            
    return curr_hist

    

###### TRADE FUNCTIONS #########


def scaled_flow_imbal(orders): 
    buys = []
    sells = []
    for i in orders[:-1]:
        if i['side'] == 'buy':
            buys.append(math.sqrt(float(i['price'])*float(i['size'])))
        elif i['side'] == 'sell':
            sells.append(math.sqrt(float(i['price'])*float(i['size'])))

    flow_imbal = sum(sells) - sum(buys) #<--taker orders (diff from polo)
    if flow_imbal > 0:
        flow = 1
    else:
        flow = -1
    return flow

def hurst(ts):

    """Returns the Hurst Exponent of the time series vector ts"""
    # Create the range of lag values
    lags = range(2, 100)

    # Calculate the array of the variances of the lagged differences
    # Here it calculates the variances, but why it uses 
    # standard deviation and then make a root of it?
    tau = [sqrt(std(subtract(ts[lag:], ts[:-lag]))) for lag in lags]

    # Use a linear fit to estimate the Hurst Exponent
    poly = polyfit(log(lags), log(tau), 1)

    # Return the Hurst exponent from the polyfit output
    if poly[0]*2.0 > 0:
        exp = poly[0]*2.0
    else:
        exp = 0.5

    return exp
    #return poly[0]*2.0

def depth(orders,est_volumne):

    base_amt = []
    for x in orders:
        base = sum([float(x[0])*float(x[1])])
        if sum(base_amt) < est_volumne:
            base_amt.append(base)  
    return len(base_amt)



if __name__ == '__main__':
    if len(sys.argv) == 6:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    else:
        print "error: -m hfs.triangular_arbitrage <api_key> <secret> <passphrase> <minimum_window> <trade_amount_btc>"
        sys.exit(2)

