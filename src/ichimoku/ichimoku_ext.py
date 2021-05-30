# /*
# De xuat he thong giao dich theo Ichimokucloud
# I. Cac thong du lieu dau vao co ban
# 1. TK= Duong Tenkan trung binh dinh day 9 phien
# 2. KJ= Duong Kijun trung binh dinh day 17 phien
# 3. SpanA = (TK+KJ)/2
# 4. SpanB= Duong trung binh dinh day 26 phien
# 5. Chiklou= Duong gia dong cua hien tai dua ve qua khu 25 phien
# 6. KJ65 - Duong trung binh dinh day 65 phien (KJ 4TF)
# 7. KJ129- Duong trung binh dinh day 129 (24TF)
# 9. Dinh nghia Kumoclound=khoang khong gian SpanA va SpanB
# 10. Kumosen - khoang khong gian giua Tenkan va Kijun
# II. Dinh nghia xu huong trong ichomoku
# 1. uptren khi kumosen> Kumoclound
# 2. Downtren khi Kumosen<=Kumoclound
# 3. Notrend khi Kumo xen kep nhau
# II. Cac hien tuong trong Ichimoku
# 1. Hien tuong hop bich/ the hien dinh day cua cac duong KJ trung nhau va cung nhau di len la luc dang co mot xu huong rat manh dang dien ra
# 2. Chikoubreakout // là tin hieu co phieu vuot thoat va buoc sang mot giai doan tang truong manh
# III. Chien luoc giao dich ua thich
# 1. Strong buy: DK1, DK2, DK3 dong thoi xay ra
# DK1: Chikou vuot may Kumoclound va Kumosen (cat len tren ca 2 clound)
# DK2: Tenkan hoac Kijun huong len (tenkan va kijun khong huong xuong)
# DK3: Gia dong cua >= KJ65
# 2. Strong sell thoát kho hoac nghiem cam mua khi tin hieu nguoc lai strong sell
# */
#
# /*----------------------------------------------------------
# Version 1.0
# Last updated: 28/05/2020 - by Phan Hieu
# -----------------------------------------------------------*/
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.dates import date2num
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle


def ichimoku_ext(df: pd.DataFrame, tenkan=9, kijun=17, spanBPeriod=26, chikoulagging=17, kijun65=65, kijun129=129, forward=25, combinationRatio=5, takeProfit=25):
    dataframe = df.copy()

    # TK = (dataframe['high'].rolling(window=tenkan).max() + dataframe['low'].rolling(window=tenkan).min()) / 2
    # KJ = (dataframe['high'].rolling(window=kijun).max() + dataframe['low'].rolling(window=kijun).min()) / 2
    # KS = (dataframe['high'].rolling(window=kijun65).max() + dataframe['low'].rolling(window=kijun65).min()) / 2
    # KS2 = (dataframe['high'].rolling(window=kijun129).max() + dataframe['low'].rolling(window=kijun129).min()) / 2
    # SpanA = ((KJ + TK) / 2)
    # SpanB = (dataframe['high'].rolling(window=spanBPeriod).max() + dataframe['low'].rolling(window=spanBPeriod).min()) / 2
    # CK = dataframe['close']

    # To compare with Chikou (=current Close)
    # SpanACS = SpanA.shift(-2*forward)
    # SpanBCS = SpanB.shift(-2*forward)
    # TKCS = TK.shift(-forward)
    # KJCS = KJ.shift(-forward)
    # UpperKumo = np.maximum(np.maximum(TKCS, KJCS), np.maximum(SpanACS, SpanBCS))
    # LowerKumo = np.minimum(np.minimum(TKCS, KJCS), np.minimum(SpanACS, SpanBCS))

    dataframe['tenkan'] = (dataframe['high'].rolling(window=tenkan).max() + dataframe['low'].rolling(window=tenkan).min()) / 2
    dataframe['kijun'] = (dataframe['high'].rolling(window=kijun).max() + dataframe['low'].rolling(window=kijun).min()) / 2
    dataframe['kijun65'] = (dataframe['high'].rolling(window=kijun65).max() + dataframe['low'].rolling(window=kijun65).min()) / 2
    dataframe['kijun129'] = (dataframe['high'].rolling(window=kijun129).max() + dataframe['low'].rolling(window=kijun129).min()) / 2
    dataframe['span_a'] = ((dataframe['kijun'] + dataframe['tenkan']) / 2)
    dataframe['span_b'] = (dataframe['high'].rolling(window=spanBPeriod).max() + dataframe['low'].rolling(window=spanBPeriod).min()) / 2
    dataframe['chikou'] = dataframe['close']

    dataframe['span_a_cs'] = dataframe['span_a'].shift(-2 * forward)
    dataframe['span_b_cs'] = dataframe['span_b'].shift(-2 * forward)
    dataframe['tenkan_cs'] = dataframe['tenkan'].shift(-forward)
    dataframe['kijun_cs'] = dataframe['kijun'].shift(-forward)
    dataframe['upper_kumo'] = np.maximum(np.maximum(dataframe['tenkan_cs'], dataframe['kijun_cs']), np.maximum(dataframe['span_a_cs'], dataframe['span_b_cs']))
    dataframe['lower_kumo'] = np.minimum(np.minimum(dataframe['tenkan_cs'], dataframe['kijun_cs']), np.minimum(dataframe['span_a_cs'], dataframe['span_b_cs']))

    return dataframe


def draw_candlesticks(ax, df):
    """ Add candlestick visuals to a matplotlib graph """

    df = df[["date", "open", "high", "low", "close"]].dropna()
    df['date'] = pd.to_datetime(df['date'])

    lines = []
    patches = []

    for i, (date, _open, high, low, close) in df.iterrows():
        date = date2num(date)
        if close >= _open:
            color = "g"
            lower = _open
            height = close - _open
        else:
            color = "r"
            lower = close
            height = _open - close

        vline = Line2D(
            xdata=(date, date),
            ydata=(low, high),
            color=color,
            linewidth=0.5,
            antialiased=True,
        )

        rect = Rectangle(
            xy=(date - 0.4, lower),
            width=0.8,
            height=height,
            facecolor=color,
            edgecolor=color,
            alpha=1.0,
        )

        lines.append(vline)
        ax.add_line(vline)
        patches.append(rect)
        ax.add_patch(rect)

    ax.autoscale_view()
    return lines, patches


def ichimoku_plot(dataframe):
    fig, ax = plt.subplots(figsize=(30, 10))

    # dataframe = dataframe[["tenkan", "kijun", "span_a", "span_b", 'volume']].dropna()
    x = dataframe["date"]
    # try:
    #     x = pd.to_datetime(x)
    # except:
    #     x = range(len(dataframe))

    # dataframe['tenkan'].dropna()

    plt.plot(x, dataframe["tenkan"], color="blue")
    plt.plot(x, dataframe["kijun"], color="maroon")
    plt.plot(x, dataframe["span_a"], color="green", linewidth=0.5)
    plt.plot(x, dataframe["span_b"], color="red", linewidth=0.5)

    draw_candlesticks(ax, dataframe)

    fig.suptitle("Ichimoku {}".format(dataframe['code'].iloc[0]), fontsize=30)
    plt.ylabel("Price ($)")
    plt.rc("axes", labelsize=20)
    plt.rc("font", size=18)

    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d/%y"))
    plt.xticks(rotation=45)

    plt.fill_between(
        x,
        dataframe["span_a"],
        dataframe["span_b"],
        where=dataframe["span_a"] >= dataframe["span_b"],
        facecolor="limegreen",
        interpolate=True,
    )
    plt.fill_between(
        x,
        dataframe["span_a"],
        dataframe["span_b"],
        where=dataframe["span_a"] <= dataframe["span_b"],
        facecolor="salmon",
        interpolate=True,
    )

    ax_vol = ax.twinx()
    vol_chart = ax_vol.bar(dataframe['date'], dataframe['volume'], width=0.25, color='blue', label=[])
    ax_vol.set_ylim(0, 3 * dataframe['volume'].max())  # it kinda squishes down the volume scale
    fig.autofmt_xdate()
    return plt.show()


def main():
    ichimoku_ext()
    print('... working')


if __name__ == '__main__':
    main()
