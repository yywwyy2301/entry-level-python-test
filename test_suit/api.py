import datetime

from PyQuantKit import MarketData
from PyQuantKit import TradeData, TickData, OrderBook
import datetime
import os
import csv


def loader(market_date: datetime.date, ticker: str, dtype: str) -> list[MarketData]:
    directory = f"Res/{market_date.strftime('%Y-%m-%d')}"
    if dtype == 'TradeData':
        path = f"{directory}/transactions/{ticker}.csv"
        data_list = []
        with open(path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data_list.append(TradeData(**row))
        return data_list
    elif dtype == 'TickData':
        path = f"{directory}/ticks/{ticker}.csv"
        data_list = []
        with open(path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data_list.append(TickData(**row))
        return data_list
    elif dtype == 'TransactionData':
        # 实现TransactionData的加载逻辑
        pass
    else:
        raise ValueError("Unsupported data type")

def test_loader():
    market_date = datetime.date(2024, 3, 8)
    ticker = '000004.SZ'
    data = loader(market_date, ticker, 'TradeData')
    print(data)

if __name__ == '__main__':
    test_loader()