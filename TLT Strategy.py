import pandas as pd
from numpy import array
import talib
import datetime
import pytz

from quantopian.pipeline.factors import RSI
from zipline.utils import tradingcalendar as calendar
from zipline.utils.tradingcalendar import get_early_closes

PRINT_BARS = not True
PRINT_TRADE_TIMES = True
PRINT_ORDERS = True

def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    # Create list of "triplets"
    # Each triplet group must be it's own list with the security to be traded as the first security of the list
    context.security_group = list()
    context.security_group.append([sid(23921), sid(16581), sid(25752)])

    # Create list of only the securities that are traded
    context.trade_securities = list()
    context.intraday_bars = dict()
    context.last = dict()
    context.stocks = list()

    for group in context.security_group:
        # Verify the group has exactly 3 listed securities
        if len(group) != 3: raise ValueError('The security group to trade {} must have 3 securities.'.format(group[0].symbol))
        
        # Add first security only to the traded list (if not already added)
        if group[0] not in context.trade_securities: context.trade_securities.append(group[0])
        
        # Loop through all stocks in triplet group
        for stock in group:
            # Add stock to context.stocks (if not already added) and create dictionary objects to hold intraday bars/last prices
            if stock not in context.stocks:
                context.stocks.append(stock) 
                context.intraday_bars[stock] = [[],[],[],[]] # empty list of lists (for ohlc prices)
                context.last[stock] = None                   # set last price to None

    # Define time zone for algorithm
    context.time_zone = 'US/Eastern' # algorithm time zone
    
    # Define the benchmark (used to get early close dates for reference).
    context.spy = sid(8554) # SPY
    start_date = context.spy.security_start_date
    end_date   = context.spy.security_end_date
    
    # Get the dates when the market closes early:
    context.early_closes = get_early_closes(start_date,end_date).date

    # Create variables required for each triplet
    context.PT = dict()
    context.SL = dict()
    context.Highest = dict()
    context.Lowest = dict()
    context.bars_since_entry = dict()
    context.prof_x = dict()
    context.entry_price = dict()
    
    context.o1 = dict()
    context.o2 = dict()
    context.o3 = dict()
    context.o4 = dict()
    context.o5 = dict()
    context.o6 = dict()
    context.ord1 = dict()
    context.ord2 = dict()
    context.max_daily_entries = dict()
    context.daily_entries = dict()
    context.max_pnl = dict()
    context.min_pnl = dict()
    context.closed_pnl = dict()
    context.trading_periods = dict()
    context.status = dict()
    context.positions_closed = dict()
    context.exit_at_close = dict()
    context.time_to_close = dict()
    context.reset_filter = dict()
    context.reset_minutes = dict()
    
    context.long_position = dict()
    context.execution_market_close = dict()
    context.execution_market_open = dict()
    context.long_on = dict()
    context.PT_ON = dict()
    context.SL_ON = dict()
    context.HH = dict()
    context.LL = dict()
    context.profitable_closes = dict()
    context.max_time = dict()
    context.position_amount = dict()

    i = 0
    for group in context.security_group:
        context.Highest[i] = 100000.0
        context.Lowest[i] = 0.0
        context.bars_since_entry[i] = 0
        context.prof_x[i] = 0    
        context.entry_price[i] = None
        
        context.o1[i] = 0
        context.o2[i] = 0
        context.o3[i] = 0
        context.o4[i] = 0
        context.o5[i] = 0
        context.o6[i] = 0
        context.ord1[i] = None
        context.ord2[i] = None
        context.daily_entries[i] = 0
        context.closed_pnl[i] = 0 # keep track of individual system profits
        context.status[i] = 'NOT TRADING' # status of the algo: 'NOT TRADING' or 'TRADING'
        context.positions_closed[i] = False # keep track of when pnl exits are triggered

        if i == 0:
            context.execution_market_close[i] = False
            context.execution_market_open[i] = True
            context.long_on[i] = 1 # 1 = long, 0 = short
            context.PT_ON[i] = 0
            context.SL_ON[i] = 1
            context.HH[i] = 0
            context.LL[i] = 0
            context.profitable_closes[i] = 1000001
            context.max_time[i] = 2
            context.position_amount[i] = 40
            context.max_daily_entries[i] = 999999
            context.max_pnl[i] = 9999999
            context.min_pnl[i] = -9999999
            context.trading_periods[i] = ['00:00-23:59'] # format: 'HH:MM-HH:MM'
            context.reset_filter[i] = False # Turns on/off resetting the filter variables
            context.reset_minutes[i] = 1 # number of minutes after market open to call reset_trade_filters()
            context.exit_at_close[i] = False # Turns on/off auto exit x minutes before market close
            context.time_to_close[i] = 1 # minutes prior to market close to exit all positions

        else:
            raise ValueError('No input variables were defined for triplet, index # {}'.format(i))
            
        if context.long_on[i]: # long only strategy
            context.PT[i] = 100000.0
            context.SL[i] = 0.0
        else: # short only strategy
            context.PT[i] = 0.0
            context.SL[i] = 100000.0
        
        # Update index i
        i += 1
        
    # Rebalance intraday. 
    context.minute_counter = 0
    context.intraday_bar_length = 5 # number of minutes for the end of day function to run
    
    # Run my_schedule_task and update bars based on configured inputs above
    for i in range(1, 390):
        if i % context.intraday_bar_length == 0: # bar close
            schedule_function(get_intraday_bar, date_rules.every_day(), time_rules.market_open(minutes=i)) # update bars
            if True in context.execution_market_close.values(): # check for True in dictionary
                schedule_function(my_schedule_task, date_rules.every_day(), time_rules.market_open(minutes=i))
            
        if i % context.intraday_bar_length == 1: # bar open
            if True in context.execution_market_open.values(): # check for True in dictionary
                schedule_function(my_schedule_task, date_rules.every_day(), time_rules.market_open(minutes=i))

   
def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    i = 0
    for group in context.security_group:
        context.ord1[i] = None
        context.ord2[i] = None
        reset_trade_filters(context, i)
        i += 1
    context.minute_counter = 0
    context.date = get_datetime(context.time_zone)#.strftime("%H%M"))


def reset_trade_filters(context, index):
    """
    Called every day at preset time.
    """
    i = 0
    for group in context.security_group:
        if i == index:
            log.info('Reseting all trade filters for triplet {}'.format(i))
            context.daily_entries[i] = 0
            context.closed_pnl[i] = 0
            context.positions_closed[i] = False
            return
        else: i += 1


def get_intraday_bar(context, data):
    """
    Function calculates historical ohlcv bars for a custom intraday period.
    """
    # Loop through all assets and print the intraday bars
    for stock in context.stocks:

        # Get enough data to form the past 30 intraday bars
        bar_count = context.intraday_bar_length * 30

        # Get bars for stock
        df = data.history(stock, ['open', 'high', 'low', 'close', 'volume'], bar_count, '1m')
        # Resample dataframe for desired intraday bar
        resample_period = str(context.intraday_bar_length) + 'T'
        result = df.resample(resample_period, base = 1).first()
        result['open'] = df['open'].resample(resample_period, base = 1).first()
        result['high'] = df['high'].resample(resample_period, base = 1).max()
        result['low'] = df['low'].resample(resample_period, base = 1).min()
        result['close'] = df['close'].resample(resample_period, base = 1).last()
        result['volume'] = df['volume'].resample(resample_period, base = 1).sum()
        # Remove nan values
        result = result.dropna()

        if PRINT_BARS: log.info('{} {} minute bar: open={}, high={}, low={}, close={}, volume={}'.format(stock.symbol, context.intraday_bar_length, result['open'][-1], result['high'][-1], result['low'][-1], result['close'][-1], result['volume'][-1]))

        # Save bars for stock to context dictionary
            context.intraday_bars[stock] = [result['open'], result['high'], result['low'], result['close']]

    
def my_schedule_task(context,data):
    """
    Execute orders according to our schedule_function() timing.
    """
    # Check if bar open or close
    if context.minute_counter % context.intraday_bar_length == 1: market_close = False # bar open
    else: market_close = True
        
    # Loop through security groups
    i = 0
    for stocks in context.security_group:
        
        # Do not continue if all positions have been closed for triplet
        if context.positions_closed[i]: return 
        
        # Do not run if not during trading hours
        if not time_to_trade(context.trading_periods[i], context.time_zone): return # Trading is not allowed
        
        # Verify daily entry limit hasn't been reached
        if context.daily_entries[i] >= context.max_daily_entries[i]: continue # go to next group of stocks

        # Check if schedule task should be run for stock
        if market_close:
            # Check if schedule function for stock should be run on bar close
            if not context.execution_market_close[i]: continue # go to next group of stocks
        else: # market open
            # Check if schedule function for stock should be run on bar open
            if not context.execution_market_open[i]: continue # go to next group of stocks
        
        # Try to get prices for all stocks
        try:
            # Get prices for first stock of triplet
            P = context.last[stocks[0]]
            stock0_prices = context.intraday_bars[stocks[0]]
            O = array(stock0_prices[0])
            H = array(stock0_prices[1])
            L = array(stock0_prices[2])
            C = array(stock0_prices[3])
            V = data.history(stocks[0], "volume", bar_count=20, frequency=tf)

            CAvg = C[-3:].mean()
            highest = H[-5:].max()
            lowest = L[-5:].min()
            atr = talib.ATR(H,L,C,20)[-1]
    
            # Get prices for second stock of triplet
            P2 = context.last[stocks[1]]
            stock1_prices = context.intraday_bars[stocks[1]]
            O2 = array(stock1_prices[0])
            H2 = array(stock1_prices[1])
            L2 = array(stock1_prices[2])
            C2 = array(stock1_prices[3])
            V2 = data.history(stocks[1], "volume", bar_count=20, frequency=tf)
                                             
            # Get prices for third stock of triplet
            P3 = context.last[stocks[2]]
            stock2_prices = context.intraday_bars[stocks[2]]
            O3 = array(stock2_prices[0])
            H3 = array(stock2_prices[1])
            L3 = array(stock2_prices[2])
            C3 = array(stock2_prices[3])
            V3 = data.history(stocks[2], "volume", bar_count=20, frequency=tf)
        except: continue # go to next group of stocks

        if i == 0:
            signal1 = (H[-1] * L[-1]) ** .5 <= (H[-3] * L[-3]) ** .5
            signal2 = hurst(H,L,C,20)[-1] > hurst(H,L,C,20)[-6]
        else:
            raise ValueError('No signals were defined for triplet, index # {}'.format(i))

        Condition1 = signal1 and signal2
        
        if context.HH[i]: context.o3[i] = 1  
        if context.LL[i]: context.o4[i] = 1  
        if context.PT_ON[i]: context.o5[i] = 1  
        if context.SL_ON[i]: context.o6[i] = 1
         
        checkZeroOrders = context.portfolio.positions[stocks[0]].amount==0
        
        if checkZeroOrders and Condition1:
            if context.long_on[i]: # long only strategy
                if context.PT_ON[i]: context.PT[i] = C[-1] + atr * 2.00
                if context.SL_ON[i]: context.SL[i] = C[-1] - atr * 2.00
                order(stocks[0], context.position_amount[i])
                if PRINT_ORDERS: log.info("Bought to open {} at price {}".format(stocks[0].symbol,P))
                context.daily_entries[i] += 1
                
            else: # short only strategy
                if context.PT_ON[i]: context.PT[i] = C[-1] - atr * 2.00
                if context.SL_ON[i]: context.SL[i] = C[-1] + atr * 2.00
                order(stocks[0], -context.position_amount[i])
                if PRINT_ORDERS: log.info("Sold to open {} at price {}".format(stocks[0].symbol, P))
                context.daily_entries[i] += 1

            context.entry_price[i] = P
            context.bars_since_entry[i] = 0
            context.prof_x[i] = 0

        if context.HH[i]: context.Highest[i] = highest[-1]
        if context.LL[i]: context.Lowest[i] = lowest[-1]

        if not checkZeroOrders: # have an open position
            co1 = 0
            co2 = 0
            context.bars_since_entry[i] += 1
            if context.bars_since_entry[i] > 0:
                if context.long_on[i] and P >= context.entry_price[i]:
                    context.prof_x[i] += 1
                if not context.long_on[i] and P <= context.entry_price[i]:
                    context.prof_x[i] += 1

            if context.prof_x[i] >= context.profitable_closes[i]: co1 = 1
            if context.bars_since_entry[i] >= context.max_time[i]: co2 = 1

            if co1 or co2:
                # Cancel open orders for triplet
                if context.ord1[i]:
                    cancel_order(context.ord1[i])
                    context.ord1[i] = None
                if context.ord2[i]:
                    cancel_order(context.ord2[i])
                    context.ord2[i] = None

                # Close current position, if one
            if context.entry_price[i] and context.long_on[i]:
                    order(stocks[0], -context.position_amount[i])
                    if PRINT_ORDERS: log.info("Sold to close {} at price {}".format(stocks[0].symbol, P))
                    context.closed_pnl[i] += ((P - context.entry_price[i]) * context.position_amount[i])

                elif context.entry_price[i]:
                    order(stocks[0], context.position_amount[i])
                    if PRINT_ORDERS: log.info("Bought to close {} at price {}".format(stocks[0].symbol, P))
                    context.closed_pnl[i] += ((context.entry_price[i] - P) * context.position_amount[i])

                context.bars_since_entry[i] = 0
                context.prof_x[i] = 0

        # Update index i
        i += 1

def handle_data(context,data):
    """
    Called every minute.
    """
    context.minute_counter += 1 # increment minute counter
    
    # Get the current exchange time, in local timezone: 
    dt = pd.Timestamp(get_datetime()).tz_convert(context.time_zone) # pandas.tslib.Timestamp type

    # Get last prices for all stocks
    for stock in context.stocks:
        context.last[stock] = data.current(stock, 'price')
    
    # Get tradeable securities only
    stocks = context.trade_securities
    Last = data.history(stocks, "price", bar_count=20, frequency='1m')
    
    i = 0
    for group in context.security_group:
        
        stock = group[0] # tradeable security of group
        P = Last[stock]

        # Check if the trade filter should be reset for the triplet
        if context.reset_filter[i] and context.reset_minutes[i] == context.minute_counter: reset_trade_filters(context, i)
         
        # Check if it is time to close the triplet positions for the end of the day
        if before_close(context, dt, context.time_to_close[i]):
            if not context.positions_closed[i]:
                log.info('Time to close all positions for {}, triplet {} for the end of the day'.format(stock.symbol, i))
                close_triplet_positions(context, i)
            return
            
        # Do not continue if all positions have been closed for triplet
        if context.positions_closed[i]: return 
        
        # Check whether trading is allowed or not
        prev_status = context.status[i]
        if time_to_trade(context.trading_periods[i], context.time_zone): # Trading is allowed
            context.status[i] = 'TRADING'
            if PRINT_TRADE_TIMES and prev_status != context.status[i]: log.info('Trading has started for {}, triplet {}.'.format(stock.symbol, i))
        else: # Trading is not allowed
            if context.status[i] == 'TRADING':
                if PRINT_TRADE_TIMES: log.info('Trading has stopped for {}, triplet {}.'.format(stock.symbol, i))
                close_triplet_positions(context, i)
            context.status[i] = 'NOT TRADING'

        # Get current pnl and exit all positions if limits are reached
        open_position_pnl = 0
        if context.entry_price[i]:
            if context.long_on[i]: open_position_pnl = ((P[-1]-context.entry_price[i])*context.position_amount[i])
            else:
                open_position_pnl = ((context.entry_price[i]-P[-1])*context.position_amount[i])
        current_pnl = open_position_pnl + context.closed_pnl[i]       
        
        if current_pnl >= context.max_pnl[i] or current_pnl <= context.min_pnl[i]:
            close_triplet_positions(context, i)
        
        # Check for exit signals
        if context.portfolio.positions[stock].amount > 0: # open long position 
            if context.ord1[i] is None and context.ord2[i] is None:
                if context.long_on[i]: # long only strategy
                    co3 = context.o3[i] and P[-1] > context.Highest[i]
                    co4 = context.o4[i] and P[-1] < context.Lowest[i]
                    co5 = context.o5[i] and P[-1] > context.PT[i]
                    co6 = context.o6[i] and P[-1] < context.SL[i]
                    LimitPr = round(min(context.Highest[i], context.PT[i]),2)
                    StopPr = round(max(context.Lowest[i], context.SL[i]),2)
                    
                    #log.info("Checking {0} at limit price {1} and Stop price {2}".format(stock,LimitPr,StopPr)) 
                    
                    if co3 or co5:
                        context.ord1[i] = order_target(stock,0,style=LimitOrder(LimitPr))
                        if PRINT_ORDERS: log.info("Sold to close {} at limit price {}".format(stock.symbol,LimitPr))
                        context.closed_pnl[i] += ((P[-1]-context.entry_price[i])*context.position_amount[i])    
                    elif co4 or co6:
                        context.ord2[i] = order_target(stock,0,style=StopOrder(StopPr))
                        if PRINT_ORDERS: log.info("Sold to close {} at stop price {}".format(stock.symbol,StopPr))
                        context.closed_pnl[i] += ((P[-1]-context.entry_price[i])*context.position_amount[i])
                           
                else: # short only strategy
                    log.info("ERROR: INVALID LONG POSITION FOR SHORT {} TRIPLET".format(stock.symbol))
      
        elif context.portfolio.positions[stock].amount < 0: # open short position
            if context.ord1[i] is None and context.ord2[i] is None:
                if not context.long_on[i]: # short only strategy
                    co3 = context.o3[i] and P[-1] > context.Highest[i]
                    co4 = context.o4[i] and P[-1] < context.Lowest[i]
                    co5 = context.o5[i] and P[-1] < context.PT[i]
                    co6 = context.o6[i] and P[-1] > context.SL[i]   
                    LimitPr = round(min(context.Lowest[stock], context.PT[stock]),2)
                    StopPr = round(max(context.Highest[stock], context.SL[stock]),2)
                    
                    #log.info("Checking {0} at limit price {1} and Stop price {2}".format(stock,LimitPr,StopPr))
                    
                    if co3 or co5:
                        context.ord1[i] = order_target(stock,0,style=LimitOrder(LimitPr))
                        if PRINT_ORDERS: log.info("Bought to close {} at limit price {}".format(stock.symbol,LimitPr))
                        context.closed_pnl[i] += ((context.entry_price[i]-P[-1])*context.position_amount[i])
                    elif co4 or co6:
                        context.ord2[i] = order_target(stock,0,style=StopOrder(StopPr))
                        if PRINT_ORDERS: log.info("Bought to close {} at stop price {}".format(stock.symbol,StopPr))
                        context.closed_pnl[i] += ((context.entry_price[i]-P[-1])*context.position_amount[i])
 
                else: # long only strategy
                    log.info("ERROR: INVALID SHORT POSITION FOR LONG {} TRIPLET".format(stock.symbol))

        else: # no open position
            #log.info("Resetting parameters {0}".format(stock))
            context.ord1[i] = None
            context.ord2[i] = None
            context.entry_price[i] = None
            context.Highest[i] = 100000.0
            context.Lowest[i] = 0.0
            if context.long_on[i]: # long only strategy
                context.PT[i] = 100000.0
                context.SL[i] = 0.0
            else: # short only strategy
                context.PT[i] = 0.0
                context.SL[i] = 100000.0
            
        # Update index i
        i += 1
        
def Cube_HLC(H,L,C):
    return (H * L * C) ** (1. / 3.)


def time_to_trade(periods, time_zone):
    """
    Check if the current time is inside trading intervals specified by the periods parameter.

    :param periods: periods
    :type periods: list of strings in 'HH:MM-HH:MM' format
    :param time_zone: Time zone
    :type time_zone: string

    returns: True if current time is with a defined period, otherwise False
    """
    # Convert current time to HHMM int
    now = int(get_datetime(time_zone).strftime("%H%M"))

    for period in periods:
        # Convert "HH:MM-HH:MM" to two integers HHMM and HHMM
        splitted = period.split('-')
        start = int(''.join(splitted[0].split(':')))
        end = int(''.join(splitted[1].split(':')))

        if start <= now < end: return True
    return False


def close_triplet_positions(context, index):
    """
    Cancel any open orders and close any positions for a given triplet.
    """
    i = 0
    for group in context.security_group:
        if i == index:
            stock = group[0]
            P = context.last[stock]
            log.info('Cancelling open orders and closing any positions for triplet {}'.format(i))
            # Cancel open orders for triplet
            if context.ord1[i]:
                cancel_order(context.ord1[i])
                context.ord1[i] = None
            if context.ord2[i]:
                cancel_order(context.ord2[i])
                context.ord2[i] = None
            
            # Close current position, if one
            if context.entry_price[i] and context.long_on[i]:
                order(stock, -context.position_amount[i])
                if PRINT_ORDERS: log.info("Sold to close {} at price {}".format(stock.symbol,P))
                context.closed_pnl[i] += ((P-context.entry_price[i])*context.position_amount[i])
                    
            elif context.entry_price[i]:
                order(stock, context.position_amount[i])
                if PRINT_ORDERS: log.info("Bought to close {} at price {}".format(stock.symbol,P))
                context.closed_pnl[i] += ((context.entry_price[i]-P)*context.position_amount[i])
            
            context.bars_since_entry[i] = 0
            context.prof_x[i] = 0

            # Set positions_closed variable to True
            context.positions_closed[i] = True
            return
            
        else: i += 1
            
            
def before_close(context, dt, minutes=0, hours=0):
    '''
    Determine if it is a variable number of hours/minutes before the market close.
    dt = pandas.tslib.Timestamp
    
    Trading calendar source code
    https://github.com/quantopian/zipline/blob/master/zipline/utils/calendars/trading_calendar.py
    '''
    tz = pytz.timezone(context.time_zone)
    
    date = get_datetime().date()
    # set the closing hour
    if date in context.early_closes: close_hr = 13 # early closing time (EST)
    else: close_hr = 16 # normal closing time (EST)
        
    close_dt = tz.localize(datetime.datetime(date.year, date.month, date.day, close_hr, 0, 0)) # datetime.datetime with tz
    close_dt = pd.Timestamp(close_dt) # convert datetime.datetime with tz to pandas.tslib.Timestamp

    delta_t = datetime.timedelta(minutes=60*hours + minutes)
    
    return dt > close_dt - delta_t

def hurst(H,L,C,_len=20):
	atr = talib.ATR(H.values,L.values,C.values,_len)
	hurst = (np.log(H.rolling(_len).max() - L.rolling(_len).min()) - np.log(atr))/np.log(_len)
	return hurst

