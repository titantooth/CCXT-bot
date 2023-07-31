import pandas as pd
import numpy as np
import time
import ccxt
from threading import Thread


class CCXTSpotTrader():  # based on Long-Short Trader (Contrarian Strategy)

    def __init__(self, symbol, bar_length, return_thresh, volume_thresh,
                 units, position=0, sandbox=True):

        exchange.set_sandbox_mode(sandbox)  # NEW!

        self.symbol = symbol
        self.bar_length = bar_length
        self.get_available_intervals()
        self.units = units
        self.position = position
        self.trades = 0
        self.trade_values = []

        # *****************add strategy-specific attributes here******************
        self.return_thresh = return_thresh
        self.volume_thresh = volume_thresh
        # ************************************************************************

    def get_available_intervals(self):

        l = []
        for key, value in exchange.timeframes.items():
            l.append(key)
        self.available_intervals = l

    def start_trading(self, start=None, hist_bars=None):

        if not hist_bars:
            hist_bars = 1000

        if self.bar_length in self.available_intervals:
            self.get_most_recent(symbol=self.symbol, interval=self.bar_length,
                                 start=start, limit=hist_bars)
            thread = Thread(target=self.start_kline_stream, args=(self.stream_candles, self.symbol, self.bar_length))
            thread.start()

        # "else" to be added later in the course

    def get_most_recent(self, symbol, interval, start, limit):

        if start:
            start = exchange.parse8601(start)

        data = exchange.fetchOHLCV(symbol=symbol, timeframe=interval, since=start, limit=limit)
        last_bar_actual = data[-1][0]

        # timestamp of current bar
        last_bar_target = exchange.fetchOHLCV(symbol=symbol, timeframe=interval, limit=2)[-1][0]

        # as long as we don´t have all bars (most recent): let´s pull the next 1000 bars
        while last_bar_target != last_bar_actual:
            time.sleep(0.1)
            data_add = exchange.fetchOHLCV(symbol=symbol, timeframe=interval, since=last_bar_actual, limit=limit)
            data += data_add[1:]
            last_bar_actual = data[-1][0]
            last_bar_target = exchange.fetchOHLCV(symbol=symbol, timeframe=interval, limit=2)[-1][0]

        df = pd.DataFrame(data)
        df.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
        df.Date = pd.to_datetime(df.Date, unit="ms")
        df.set_index("Date", inplace=True)
        df["Complete"] = [True for row in range(len(df) - 1)] + [False]
        self.last_bar = df.index[-1]

        self.data = df

    def stream_candles(self, msg):
        # defines how to process the msg

        # extract data form msg
        start_time = pd.to_datetime(msg[-1][0], unit="ms")
        first = msg[-1][1]
        high = msg[-1][2]
        low = msg[-1][3]
        close = msg[-1][4]
        volume = msg[-1][5]

        # check if a bar is complete
        if start_time == self.last_bar:
            complete = False
        else:
            complete = True
            if len(msg) == 2:
                self.data.loc[self.last_bar] = [msg[0][1], msg[0][2], msg[0][3], msg[0][4], msg[0][5], complete]
            else:
                self.data.loc[self.last_bar, "Complete"] = complete
            self.last_bar = start_time

        # print something
        print(".", end="", flush=True)

        # feed df with latest bar
        self.data.loc[start_time] = [first, high, low, close, volume, False]

        # if a bar is complete, define strategy and trade
        if complete == True:
            self.define_strategy()
            self.execute_trades()

    def start_kline_stream(self, callback, symbol, interval):

        self.running = True

        while self.running == True:
            msg = exchange.fetchOHLCV(symbol=symbol, timeframe=interval, limit=2)

            if len(msg) == 0:
                print("No data received")
            else:
                callback(msg)

            time.sleep(1)

    def stop_stream(self):
        self.running = False

    def define_strategy(self):

        df = self.data.loc[self.data.Complete == True].copy()  # Adj!

        # ******************** define your strategy here ************************
        df = df[["Close", "Volume"]].copy()
        df["returns"] = np.log(df.Close / df.Close.shift())
        df["vol_ch"] = np.log(df.Volume.div(df.Volume.shift(1)))
        df.loc[df.vol_ch > 3, "vol_ch"] = np.nan
        df.loc[df.vol_ch < -3, "vol_ch"] = np.nan

        cond1 = df.returns <= self.return_thresh[0]
        cond2 = df.vol_ch.between(self.volume_thresh[0], self.volume_thresh[1])
        cond3 = df.returns >= self.return_thresh[1]

        df["position"] = 0
        df.loc[cond1 & cond2, "position"] = 1
        df.loc[cond3 & cond2, "position"] = -1
        # ***********************************************************************

        self.prepared_data = df.copy()

    def execute_trades(self):
        if self.prepared_data["position"].iloc[-1] == 1:  # if position is long -> go/stay long
            if self.position == 0:
                order = exchange.createMarketOrder(symbol=self.symbol, side="BUY", amount=self.units)
                self.report_trade(order, "GOING LONG")
            elif self.position == -1:
                order = exchange.createMarketOrder(symbol=self.symbol, side="BUY", amount=self.units)
                self.report_trade(order, "GOING NEUTRAL")
                time.sleep(0.1)
                order = exchange.createMarketOrder(symbol=self.symbol, side="BUY", amount=self.units)
                self.report_trade(order, "GOING LONG")
            self.position = 1
        elif self.prepared_data["position"].iloc[-1] == 0:  # if position is neutral -> go/stay neutral
            if self.position == 1:
                order = exchange.createMarketOrder(symbol=self.symbol, side="SELL", amount=self.units)
                self.report_trade(order, "GOING NEUTRAL")
            elif self.position == -1:
                order = exchange.createMarketOrder(symbol=self.symbol, side="BUY", amount=self.units)
                self.report_trade(order, "GOING NEUTRAL")
            self.position = 0
        if self.prepared_data["position"].iloc[-1] == -1:  # if position is short -> go/stay short
            if self.position == 0:
                order = exchange.createMarketOrder(symbol=self.symbol, side="SELL", amount=self.units)
                self.report_trade(order, "GOING SHORT")
            elif self.position == 1:
                order = exchange.createMarketOrder(symbol=self.symbol, side="SELL", amount=self.units)
                self.report_trade(order, "GOING NEUTRAL")
                time.sleep(0.1)
                order = exchange.createMarketOrder(symbol=self.symbol, side="SELL", amount=self.units)
                self.report_trade(order, "GOING SHORT")
            self.position = -1

    def report_trade(self, order, going):

        # extract data from order object (Adj!!!)
        side = order["side"].upper()
        time = pd.to_datetime(order["timestamp"], unit="ms")
        base_units = float(order["filled"])
        quote_units = float(order["cost"])
        price = float(order["average"])
        fee_asset = order["fees"][0]["currency"]
        fee_amount: order["fees"][0]["cost"]

        # calculate trading profits
        self.trades += 1
        if side == "BUY":
            self.trade_values.append(-quote_units)
        elif side == "SELL":
            self.trade_values.append(quote_units)

        if self.trades % 2 == 0:
            real_profit = round(np.sum(self.trade_values[-2:]), 3)
            self.cum_profits = round(np.sum(self.trade_values), 3)
        else:
            real_profit = 0
            self.cum_profits = round(np.sum(self.trade_values[:-1]), 3)

        # print trade report
        print(2 * "\n" + 100 * "-")
        print("{} | {}".format(time, going))
        print("{} | Base_Units = {} | Quote_Units = {} | Price = {} ".format(time, base_units, quote_units, price))
        print("{} | Profit = {} | CumProfits = {} ".format(time, real_profit, self.cum_profits))
        print(100 * "-" + "\n")

exchange = ccxt.binance()
api_key = "insert here"
secret_key = "insert here"
exchange.apiKey = api_key
exchange.secret = secret_key
symbol = "BTC/USDT"
bar_length = "1m"
return_thresh = [-0.0001, 0.0001]
volume_thresh = [-3, 3]
units = 0.01
position = 0
trader = CCXTSpotTrader(symbol = symbol, bar_length = bar_length, return_thresh = return_thresh,
                        volume_thresh = volume_thresh, units = units, position = 0, sandbox = True)
exchange.fetchBalance()["info"]["balances"] # get asset balances