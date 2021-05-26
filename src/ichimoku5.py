# ----------------------------------------------------------------------------- #
#
# Custom Ichimoku Cloud
#
# -----------------------------------------------------------------------------#


def ichimoku5(dataframe,
              t1=9,
              t2=17,
              t3=26,
              t4=65,
              t5=129):
    # KS_9
    KS_9 = (dataframe['high'].rolling(window=t1).max()
            + dataframe['low'].rolling(window=t1).min()) / 2

    # KS_17
    KS_17 = (dataframe['high'].rolling(window=t2).max()
             + dataframe['low'].rolling(window=t2).min()) / 2

    # KS_26
    KS_26 = (dataframe['high'].rolling(window=t3).max()
             + dataframe['low'].rolling(window=t3).min()) / 2

    # KS_65
    KS_65 = (dataframe['high'].rolling(window=t4).max()
             + dataframe['low'].rolling(window=t4).min()) / 2

    # KS_129
    KS_129 = (dataframe['high'].rolling(window=t5).max()
              + dataframe['low'].rolling(window=t5).min()) / 2

    SPAN_1 = (KS_9 + KS_17) / 2
    SPAN_2 = (dataframe['high'].rolling(window=t3).max()
              + dataframe['low'].rolling(window=t3).min()) / 2

    SPAN_A = SPAN_1.shift(-t3)
    SPAN_B = SPAN_2.shift(-t3)

    LAGGING_T2 = dataframe['close'].shift(-t2)
    LAGGING_T3 = dataframe['close'].shift(-t3)

    LAGGING_PT2M1 = dataframe['close'].shift(t2 - 1)  # lagging plus (t2 minus 1)
    LAGGING_MT2P1 = dataframe['close'].shift(-(t2 + 1))  # lagging minus (t2 plus 1)

    cloud_green = (SPAN_A > SPAN_B)
    cloud_red = (SPAN_B > SPAN_A)

    KS_17_SL_T3 = KS_17.shift(-t3)  # KS_17 shift left t3
    KS_9_SL_T3 = KS_9.shift(-t3)
    SPAN_2_SL_T2P1 = SPAN_2.shift(-(t2 + 1))  # SPAN 2 shift left t2 + 1

    return {
        'ks9': KS_9,
        'ks17': KS_17,
        'ks26': KS_26,  # extend KS_26
        'ks65': KS_65,  # extend KS_65
        'ks129': KS_129,  # extend KS_129
        'span_a': SPAN_A,
        'span_b': SPAN_B,
        'span_1': SPAN_1,
        'span_2': SPAN_2,
        'lagging_t2': LAGGING_T2,
        'lagging_t3': LAGGING_T3,  # extend
        'cloud_green': cloud_green,
        'cloud_red': cloud_red,
        'ks_9_sl_t3': KS_9_SL_T3,
        'ks_17_sl_t3': KS_17_SL_T3,
        'span_2_sl_t2p1': SPAN_2_SL_T2P1,
        'lagging_pt2m1': LAGGING_PT2M1,
        'lagging_mt2p1': LAGGING_MT2P1,
    }
