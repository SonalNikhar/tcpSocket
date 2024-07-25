import pandas as pd
maindf=pd.DataFrame()

clientDict={}

path=r'\\192.168.102.102\Techexcel\TradeAPI\TradeFO_13122023.txt'

df1=pd.read_csv(path,header=None,dtype={20:str},skiprows=0)

# tradesDict=dict(zip(df1[2], df1[12]))
#
# common_keys = set(clientDict.keys()) & set(tradesDict.keys())
#
# clientDict.update(tradesDict)
#
# print(common_keys)







maindf = pd.concat([maindf, df1],
                        axis=0,ignore_index=True)

notisSNo = maindf.shape[0]


df1 = maindf

print(df1.shape)

# df1=df1.drop_duplicates()

df1 = df1.iloc[:, [2, 4, 5, 6, 7, 12, 20, 23, 25, 26, 27, 28]]

df1.columns = ['TradeNo', 'Token', 'Qty', 'Price', 'BuySell', 'ClientID', 'TerminalID', 'Symbol',
               'Instrument',
               'EXP', 'Strike', 'OPTType']
# print('dfj',df.shape)
df = df1.drop_duplicates()

# df = df.loc[:, :]

# df.to_csv('finaldata.csv')



print('rc', df.shape)



