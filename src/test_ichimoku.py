import numpy as np
import pandas as pd

from stock import Stock
from ichimoku.ichimoku_ext import ichimoku_ext, ichimoku_plot

array1 = [1, 3, 9, 10]
array2 = [3, 6, 2, 5]
array3 = [5, 2, 6, 11]

dataframe = pd.DataFrame({'array1': array1, 'array2': array2, 'array3': array3})

ar = np.maximum(array1, array2)
ar1 = np.maximum(ar, array3)

print(ar)

print(np.maximum(dataframe['array1'], dataframe['array2']))

fpt = Stock('FPT')
fpt.load_price_board_day(length=100)
df = ichimoku_ext(fpt.df_day)
ichimoku_plot(df)

# df.to_excel('./outputs/ichimoku_ext_fpt.xlsx')

print(df)
