from curses import raw
import datetime
from operator import length_hint
import threading
from turtle import pos
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame
from numpy import diff
import pandas as pd
import time
import pytz

UTC = pytz.timezone('UTC')

# Get the current time, 15minutes, and 1 hour ago
time_now = datetime.datetime.now(tz=UTC)
time_15_min_ago = time_now - datetime.timedelta(minutes=15)
time_1_hr_ago = time_now - datetime.timedelta(minutes=150)

API_KEY = "PKN53T7SPGVUWNE07A5A"
API_KEY_SECRET = "BGix18fsq7H5qAwXMm5ugQA66vovqKw7YSuSbx1m"
BASE_URL = "https://paper-api.alpaca.markets"
#print(account)

#StockUniverse = ["SPY", "AAPL", "GOOG", "GOOGL", "AMZN", "TSLA", "MSFT", "META", "SHEL", "PSX"] #10
#PositionSize = 0.1

class Strategy:
    def __init__(self):
        self.alpaca = tradeapi.REST(API_KEY, API_KEY_SECRET, BASE_URL, api_version='v2')
        StockUniverse = ["SPY", "AAPL", "GOOG", "GOOGL", "AMZN", "TSLA", "MSFT", "META", "SHEL", "PSX"] #10
        self.allStocks = []
        for stock in StockUniverse:
            self.allStocks.append([stock, 0])
        self.long = []
        self.short= []
        self.longAmount = 0
        self.shortAmount = 0
        self.quantShort = None
        self.quantLong = None
        self.blacklist = set()
    
    def awaitMarketOpen(self):
        isOpen = self.alpaca.get_clock().is_open
        print(isOpen)
        while(not isOpen):
            clock = self.alpaca.get_clock()
            openingTime = clock.next_open.replace(tzinfo=datetime.timezone.utc).timestamp()
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            timeToOpen = int((openingTime - currTime) / 60)
            print(str(timeToOpen) + " minutes til market open.")
            time.sleep(60)
            isOpen = self.alpaca.get_clock().is_open
    
    def run(self):
        orders = self.alpaca.list_orders(status="open")
        for order in orders:
            self.alpaca.cancel_order(order.id)

        print("Wait for market to open")
        self.awaitMarketOpen()
        while True:
            clock = self.alpaca.get_clock()
            closingTime = clock.next_close.replace(tzinfo=datetime.timezone.utc).timestamp()
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            self.timeToClose = closingTime - currTime

            if(self.timeToClose < (60 * 15)):
                print("Market Closing")
                positions = self.alpaca.list_positions()
                for position in positions:
                    if(position.side == 'long'):
                        side = 'sell'
                    else:
                        side = 'buy'
                    self.submitOrder(abs(int(float(position.qty))), position.symbol, side, [])
                    time.sleep(60 * 15)
            else:
                self.rebalance()
                time.sleep(60)

    def getPercentChanges(self):
        for i, stock in enumerate(self.allStocks):
            barsActual = self.alpaca.get_bars(stock[0], TimeFrame.Minute, start=time_1_hr_ago.isoformat(), 
             end=time_15_min_ago.isoformat(), adjustment='raw')
            if len(barsActual) != 0:
                self.allStocks[i][1] = (barsActual[len(barsActual) - 1].c - barsActual[0].o) / barsActual[0].o
        self.allStocks.sort(key=lambda x: x[1])
    
    def rebalance(self):
        self.rank()
        orders = self.alpaca.list_orders(status="open")
        for order in orders:
            self.alpaca.cancel_order(order.id)
        
        print("We are taking a long position in: " + str(self.long))
        print("We are taking a short position in: " + str(self.short))

        executed = [[], []]
        positions = self.alpaca.list_positions()
        self.blacklist.clear()
        for position in positions:
            if(self.long.count(position.symbol) == 0):
                # Position is not in long list.
                if(self.short.count(position.symbol) == 0):
                # Position not in short list either.  Clear position.
                    if(position.side == "long"):
                        side = "sell"
                    else:
                        side = "buy"
                    OrderSubmit = []
                    self.submitOrder(abs(int(float(position.qty))), position.symbol, side, OrderSubmit)
                else:
                #Position is to be shorted
                    if(position.side == "long"):
                        side = "sell"
                        OrderSubmit = []
                        self.submitOrder(abs(int(float(position.qty))), position.symbol, side, OrderSubmit)
                    else:
                        if(abs(int(float(position.qty))) == self.quantShort):
                            pass
                        else:
                            additional = abs(int(float(position.qty))) - self.quantShort
                            if(additional > 0):
                                side = "buy"
                            else:
                                side = "sell"
                            OrderSubmit = []
                            self.submitOrder(abs(additional), position.symbol, side, OrderSubmit)
                            executed[1].append(position.symbol)
                            self.blacklist.add(position.symbol)
            else:
            #should be in long
                if(position.side == "short"):
                    side = "buy"
                    OrderSubmit = []
                    self.submitOrder(abs(int(float(position.qty))), position.symbol, side, OrderSubmit)
                else:
                    if(int(float(position.qty)) == self.quantLong):
                        pass
                    else:
                        additional = abs(int(float(position.qty))) - self.quantLong
                        if(additional > 0):
                            side = "sell"
                        else:
                            side = "buy"
                        OrderSubmit = []
                        self.submitOrder(abs(additional), position.symbol, side, OrderSubmit)
                        executed[0].append(position.symbol)
                        self.blacklist.add(position.symbol)
        side = "buy"
        OrderSubmit = []
        self.remOrder(self.quantLong, self.long, side, OrderSubmit)

        side = "sell"
        OrderSubmit = []
        self.remOrder(self.quantShort, self.short, side, OrderSubmit)
        
    
    def submitOrder(self, qty, stock, side, Submitted):
        try:
            self.alpaca.submit_order(stock, qty, side, "market", "day")
            print("Order " + stock + " of quantity " + str(qty) + " went through")
            Submitted.append(True)
        except:
            print("Order " + stock + " of quantity " + str(qty) + " did not go through")
            Submitted.append(False)

    def remOrder(self, qty, stocks, side, submitted):
        for individualStock in stocks:
            if(self.blacklist.isdisjoint({individualStock})):
                Ordered = []
                self.submitOrder(qty, individualStock, side, Ordered)
    def rank(self):
        self.getPercentChanges()
        SecuritiesAmount = (len(self.allStocks)) // 4
        self.long = []
        self.short = []

        for i, StockChoice in enumerate(self.allStocks):
            if(i < SecuritiesAmount):
                self.short.append(StockChoice[0])
                print("Taking short position in " + StockChoice[0])
            elif(i > (len(self.allStocks) - 1 - SecuritiesAmount)):
                self.long.append(StockChoice[0])
                print("Taking long position in " + StockChoice[0])
            else:
                continue
        
        equity = int(float(self.alpaca.get_account().equity))

        self.shortAmount = equity * 0.3
        self.longAmount = equity + self.shortAmount

        TotalPriceLong = []
        self.getPrice(self.long, TotalPriceLong)
        print(TotalPriceLong)

        TotalPriceShort = []
        self.getPrice(self.short, TotalPriceShort)
        print(TotalPriceShort)

        self.quantLong = int(self.longAmount // TotalPriceLong[0]) - 1
        self.quantShort = int(self.shortAmount // TotalPriceShort[0]) - 1

        print(self.quantLong)
    def getPrice(self, StockList, TotalPrice):
        Price = 0
        for stock in StockList:
            bar = self.alpaca.get_latest_bar(stock)
            Price += bar.c
        TotalPrice.append(Price)
strat = Strategy()
strat.run()