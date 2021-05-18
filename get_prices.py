import requests
import json
import pandas as pd
from datetime import datetime, timedelta

STOCK_URL = 'https://api.vietstock.vn/ta/history'


class StockPriceException(Exception):

    def __init__(self, status_code, status_message):
        pass


def get_prices(stock, resolution, from_date, to_date):
    """
    get_prices(stock, resolution, from_date, to_date) -> dict
    Tham số:
      - stock: mã cổ phiếu, bao gồm 3 chữ cái (hoặc số) và không chứa dấu cách
      - resolution: cho biết khung thời gian sẽ lấy về. Lấy đồ thị ngày thì điền D, lấy đồ thị giờ thì điền số giây (60, 120, 240, ....)
      - from_date: là một số mô tả thời gian bắt đầu của dữ liệu, theo chuẩn Epoch
      - to_date: là một số mô tả thời gian kết thúc của dữ liệu, theo chuẩn Epoch
    Trả về:
      - Trả về một đối tượng có kiểu dict, có key lần lượt như sau:
        - t: mảng số nguyên, mỗi phần tử là thời gian tương ứng với dữ liệu, theo chuẩn Epoch
        - h: mảng số thực, mỗi phần tử là giá cao nhất trong ngày, đơn vị VND
        - l: mảng số thực, mỗi phần tử là giá thấp nhất trong ngày, đơn vị VND
        - o: mảng số thực, mỗi phần tử là giá mở cửa, đơn vị VND
        - c: mảng số thực, mỗi phần tử là giá đóng cửa, đơn vị VND
        - v: mảng số thực, mỗi phần tử là volume trong ngày, đơn vị là cổ phiếu
    Ngoại lệ:
      - Hàm sẽ phát sinh một StockPriceException trong bất cứ trường hợp nào server trả về status code khác 200.
    """
    headers = {'Content-Type': 'application/json',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36'}

    params = {
        'symbol': stock,
        'resolution': resolution,
        'from': int(from_date),
        'to': int(to_date),
    }

    r = requests.get(STOCK_URL, headers=headers, params=params)
    if r.status_code != 200:
        raise StockPriceException(r.status_code, r.text)

    # parse data
    prices = json.loads(r.text)
    prices = json.loads(prices)
    if prices.get('s', "") != "ok":
        raise StockPriceException(600, "Invalid price")
    del prices['s']
    return prices


def get_prices_n_days(stock, days, resolution="D", time_slot_size=15):
    today = datetime.now()

    n_days_ago = today - timedelta(days=int(days / 5) * 7 + 21)
    data = pd.DataFrame()
    if days > time_slot_size > 0:
        slot = int(days / time_slot_size)
        for i in range(slot, 0, -1):
            inputs = pd.DataFrame.from_dict(get_prices(stock, resolution, n_days_ago.timestamp(), (n_days_ago + timedelta(days=time_slot_size)).timestamp()))
            data = pd.concat([data, inputs], ignore_index=True, sort=False)
            n_days_ago += timedelta(days=time_slot_size)

    inputs = pd.DataFrame.from_dict(get_prices(stock, resolution, n_days_ago.timestamp(), today.timestamp()))
    data = pd.concat([data, inputs], ignore_index=True, sort=False)
    data.drop_duplicates(inplace=True, ignore_index=True)
    data.tail(days).to_dict()
    return data.tail(days).reset_index(drop=True).to_dict()
