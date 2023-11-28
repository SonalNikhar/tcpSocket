# # import pandas as pd
# #
# # # Creating the dataframe
# # df = pd.DataFrame({"A": [12, 4, 5,12, 44, 1,12],
# #                    "B": [5, 2, 54, 5,3, 2,5],
# #                    "C": [20, 16, 7,20, 3, 8,20],
# #                    "D": [14, 3, 17, 14,2, 6,14]})
# #
# # # df1 = pd.DataFrame({"u": [ 4,12, 5, 4, 1,5],
# # #                    "i": [ 2, 5,54, 3, 2,85],
# # #                    "o": [ 16,20, 7, 3, 8,95],
# # #                    "D": [ 3, 14,17, 2, 6,96]})
# # #
# # print('dfj',df)
# # df=df.drop_duplicates()
# # df = df.iloc[2:, :]
# # print(df)
# # i1 = pd.MultiIndex.from_frame(df)
# # i2 = pd.MultiIndex.from_frame(df1)
# # print(df[~i1.isin(i2)])
#
# import pandas as pd
# no=0
# df1=pd.DataFrame()
# for i in range(3):
#
#     try:
#         df = pd.read_csv(r'C:\Users\HP\Desktop\\46_10042023.csv', header=None)
#         df1=pd.concat([df1, df],
#                   axis=0,ignore_index=True)
#         print(df1)
#
#         if df.shape[0] != 0:
#             print('no',no,df.shape[0])
#             no += df.shape[0]
#             print('no', no, df.shape[0])
#             print(df)
#     except:
#         print('kjkdf')



import mysql.connector

import mysql.connector
mydb = mysql.connector.connect(
        host="192.168.130.42",
        user="root",
        passwd="Admin@123",
        database="anvdb"
    )
mycursor = mydb.cursor()

import numpy as np
a = [[1, 2, 3], [4, 5, 6], [8, 10, 11], [15, 58, 9], [45, 53, 46]]
nd_a = np.array(a)

print(nd_a)
Delpos=0
pastePos=3
dd=nd_a[pastePos-1].copy()


nd_a[pastePos-1]=nd_a[Delpos]
nd_a[Delpos]=dd
# print(dd)
print(nd_a)

