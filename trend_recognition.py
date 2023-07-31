import flask, numpy as np, yfinance as yf, talib, plotly.graph_objects as go
import pandas as pd

l = yf.download("SPY", start ='2020-01-01', end = '2020-12-30')
data = pd.DataFrame(l)
index = data.index.tolist()

morning_star = talib.CDLMORNINGSTAR(data['Open'],data['High'],data['Low'],data['Close'])
engulfing = talib.CDLENGULFING(data['Open'],data['High'],data['Low'],data['Close'])
data['Morning Star'] = morning_star
data['Engulfing'] = engulfing
morning_star_days = data[data['Morning Star'] != 0]
engulf_days = data[data['Engulfing'] != 0]
print(engulf_days)
print(morning_star_days)


def plot_data():
    fig = go.Figure(data=[go.Candlestick(x= [i for i in index],
                    open=data['Open'],
                    high=data['High'],
                    low=data['Low'],
                    close=data['Close'])])

    fig.show()