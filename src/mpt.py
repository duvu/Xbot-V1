## https://stackoverflow.com/questions/59790285/how-to-solve-a-system-of-equations-and-constraints-for-portfolio-optimization
from get_prices import get_prices_n_days1, get_prices_n_days
import pandas as pd
import numpy as np
import scipy.optimize as optimize
import os
import json
import math
from argparse import ArgumentParser


def mock_get_prices_n_days(symbol, days):
    if os.path.exists('%s.cached' % symbol):
        return json.load(open('%s.cached' % symbol, 'rt'))
    else:
        r = get_prices_n_days(symbol, days + 1)
        json.dump(r, open('%s.cached' % symbol, 'wt'))
    return r


if os.environ.get("ENV_DEV"):
    read_prices = mock_get_prices_n_days
else:
    read_prices = get_prices_n_days


def process_symbol_data(inputs):
    inputs = pd.Series(inputs)
    profit_by_day = inputs.rolling(window=2).apply(
        lambda x: (x.iloc[1] - x.iloc[0]) / x.iloc[0])
    mean = profit_by_day.mean()
    diff = profit_by_day.rolling(window=1).apply(lambda x: x.iloc[0] - mean)
    diff.dropna(inplace=True)
    return mean, profit_by_day.std(), diff


def calc_correlation(symbols, days, progressCallback=None):
    diff = pd.DataFrame()
    mean = dict()
    std = dict()
    available_symbols = []
    for symbol in symbols:
        try:
            inputs = read_prices(symbol, days + 1)
        except:
            if progressCallback:
                progressCallback(symbol, symbols.index(symbol))
            continue
        available_symbols.append(symbol)
        closed = inputs['c']

        print('close', closed)

        m, s, d = process_symbol_data(closed)
        mean[symbol] = m
        std[symbol] = s
        diff[symbol] = d
        if progressCallback:
            progressCallback(symbol, symbols.index(symbol))

    corr = diff.T.dot(diff)
    corr = corr.apply(lambda x: x.rolling(window=1).apply(lambda x: x.iloc[0] / days))
    return available_symbols, mean, corr


def optimize_profit(symbols, mean, corr, bank_interest_rate=0.07):
    mean = pd.Series(mean)

    def objective(params):
        ratio = pd.Series(params).T
        profit = np.dot(mean, ratio)
        risk = math.sqrt(np.dot(np.dot(ratio.T, corr), ratio))
        return - (profit - bank_interest_rate / 360) / risk

    initial_guess = (1 / len(symbols),) * len(symbols)
    bounds = ((0, 1),) * len(symbols)
    constraints = ({'type': 'eq', "fun": lambda x: 1.0 - np.sum(x)})
    optimum = optimize.minimize(
        objective, initial_guess, bounds=bounds, constraints=constraints)
    result = pd.Series(optimum.x, index=symbols).apply(
        lambda x: '{:.0%}'.format(x if x >= 0.01 else 0))
    return result


def main():
    parser = ArgumentParser()
    parser.add_argument("-d", "--days", type=int, dest="days",
                        help="number of days", default=120)
    parser.add_argument('symbols', metavar='HPG', type=str, nargs='+',
                        help='your stock symbols')
    args = parser.parse_args()
    print("[+] -- Downloading stock data -- ")
    symbols, mean, corr = calc_correlation(args.symbols, args.days, lambda x, y: print("{}/{} {}".format(y + 1, len(args.symbols), x)))
    print("[+] -- Running solver -- ")
    result = optimize_profit(symbols, mean, corr)
    print(result)


if __name__ == "__main__":
    main()
