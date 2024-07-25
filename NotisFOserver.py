import datetime
import numpy as np
import time
import sys
import traceback
import json
import pickle
import pandas as pd
from PyQt5.QtCore import QByteArray, QDataStream, QIODevice
from PyQt5.QtWidgets import QApplication, QDialog,QMainWindow
from PyQt5.QtNetwork import QHostAddress, QTcpServer
from PyQt5.QtWidgets import  *
from PyQt5.QtCore import *
from pymongo import MongoClient
import os
import sqlite3
# import redis

# loc = os.getcwd().split('Application')[0]
# DBFilePath = os.path.join(loc, 'Database', 'tradeDB.db')


# r = redis.Redis(
#   host='redis-16537.c305.ap-south-1-1.ec2.cloud.redislabs.com',
#   port=16537,
#   password='n3KlqinxOyh3CIca6EfXG4fV3SO2VdeX')
# print("rrr:",r)
# #
# # r = redis.Redis(
# #     host='',
# #     port=16537,
# #     db='Sonal-free-db',
# #     password='n3KlqinxOyh3CIca6EfXG4fV3SO2VdeX')
# a = r.hset('dd','foo',1)
# print("dd",r.get('foo'))
# print("11:", a)
# b = r.hexists('dd','fo1o')
#
# print("121:", b)
# print(r)
# try:
#
#     print(r)
# except:
#     print(traceback.print_exc())
# # print(r)

# redis-cli -u redis://<username>:<password>@redis-16537.c305.ap-south-1-1.ec2.cloud.redislabs.com:16537

di = {}
# print("di[finalkey]:", di['ee'])
# di['ee'] = True
print("111di[finalkey]:", di.get('ee'))
di['ee'] = True
print("111di[finalkey]:", di['ee'])

# print("i fc:", fc)

class Server(QMainWindow):
    def __init__(self):
        super().__init__()
        self.rc = 0
        self.len=0
        self.fc = 0

        self.Expense()
        self.tcpServer = QTcpServer(self)
        PORT = 22666
        address = QHostAddress('192.168.130.42')
        # PORT = 31698
        # address = QHostAddress('127.0.0.9')
        self.tcpServer.listen(address, PORT)
        # self.alltradearr = np.empty((1000000, 17), dtype=object)


        self.today = datetime.datetime.today().strftime('%d%m%Y')
        self.path = r'\\192.168.102.102\Techexcel\TradeAPI\TradeFO_' + self.today + '.txt'
        # self.path = r'\\192.168.102.102\Techexcel\TradeAPI\TradeFO_2107
        # 2023.txt'
        self.notisSNo=0
        self.maindf=pd.DataFrame()

        self.clientList = []
        self.clientDict={}
        self.tcpServer.newConnection.connect(self.dealCommunication)
        # self.On_readyRead1()
        self.timer=QTimer()
        self.timer.setInterval(10000)
        self.timer.timeout.connect(self.On_readyRead1)

        self.tcpServer.listen(address, PORT)
        self.tcpServer.waitForNewConnection(1000000)








        # self.timer.start()
        # self.tradeFiletoNumpy()

    def Expense(self):
        mongoclient = MongoClient("192.168.130.42", 27017)
        Calculation_data = mongoclient['Calculated_data']
        Expense_collection = Calculation_data['Expense']
        self.dbExpense = list(Expense_collection.find())
        self.Expense = pd.DataFrame(self.dbExpense)[
            ['particulars', 'f_buy', 'f_sell', 'o_buy', 'o_sell', 'i_buy', 'i_sell', 'd_buy', 'd_sell']].values

        self.f_buyex = self.Expense[:, 1].sum()
        self.f_sellex = self.Expense[:, 2].sum()
        self.o_buyex = self.Expense[:, 3].sum()
        self.o_sellex = self.Expense[:, 4].sum()

    # def On_readyRead(self):
    #     try:
    #         st=time.time()
    #
    #         df1=pd.read_csv(self.path,header=None,dtype={20:str})
    #
    #
    #         if df1.shape[0]!=0:
    #
    #
    #             # print(df.columns)
    #             # print(df.dtypes)
    #             df1 = df1.iloc[:, [2, 4, 5, 6, 7,12, 20,23, 25, 26, 27, 28]]
    #
    #             df1.columns = ['TradeNo', 'Token', 'Qty', 'Price', 'BuySell', 'ClientID', 'TerminalID', 'Symbol', 'Instrument',
    #                              'EXP', 'Strike','OPTType']
    #             # print('dfj',df.shape)
    #
    #             df=df1.drop_duplicates()
    #             self.notisSNo = df1.shape[0]
    #
    #
    #             df = df.loc[self.notisSNo:, :]
    #
    #
    #             # pd.merge(contract, span1, how='left', left_on=['symbol', 'exp', 'strk1', 'opt'],
    #             #          right_on=['symbol', 'exp', 'strk', 'opt']).to_numpy()
    #
    #             # df2 = df
    #             # print(df.shape)
    #             self.rc+=df.shape[0]
    #             print('rc',self.rc)
    #
    #             convert_dict = {'TerminalID': str,'ClientID':str}
    #
    #             df = df.astype(convert_dict)
    #
    #             # print(df.dtypes)
    #
    #             # df['TerminalID'] = df['TerminalID'].astype(str)
    #
    #
    #             df["Qty"]=np.where(df['BuySell']==1, df['Qty'], -df['Qty'])
    #             df["TerminalID"]=np.where(df['TerminalID']=='0', df['ClientID'], df['TerminalID'])
    #             df["Price"]= df["Price"] / 100
    #             df["Strike"]= df["Strike"] / 100
    #             df["EXP"]= df["EXP"].apply(self.updateexp)
    #             # float(int(data[6]) / 100) * -TQty
    #
    #             df["Tradeamt"]=-df["Qty"] * df["Price"]
    #             df["Val"] = abs(df["Tradeamt"]) / 10000000
    #
    #
    #             df["TOC"] = np.where(df['OPTType']=='XX', np.where(df['BuySell']==1,(df['Val'] * self.f_buyex), (df['Val'] * self.f_sellex)),np.where(df['BuySell']==1,(df['Val'] * self.o_buyex), (df['Val'] * self.o_sellex)))
    #
    #             # print(df["Qty"])
    #             # print(df["Tradeamt"])
    #             # print(df.columns)
    #
    #
    #             # df=df.groupby(['TerminalID','Token','EXP','Strike','OPTType','Symbol','Instrument'])["Qty","Tradeamt"].sum().reset_index()
    #             df=df.groupby(['TerminalID','Token','EXP','Strike','OPTType','Symbol','Instrument']).aggregate({'Qty':'sum','Tradeamt':'sum','TOC':'sum'}).reset_index()
    #             # for i in df.to_numpy():
    #             #     # print(type(row))
    #                 # self.sgdatasend.emit(i)
    #             et=time.time()
    #             print('et',et-st)
    #
    #             # df.to_csv('d:/aaa.csv')
    #             # print('fjdf')
    #             for row in df.values.tolist():
    #
    #
    #                 for izzz in self.clientList:
    #                     row=str(row)
    #                     # print(row)
    #                     jd = row.encode('UTF-8')
    #                     # print('djf',jd)
    #                     size = len(jd)
    #
    #
    #
    #                     # print('size',size)
    #                     if (size > 200):
    #                         print('size', size)
    #                     #
    #                     blank = 200 - size
    #                     k = row + (blank * ' ')
    #                     #
    #                     jd = k.encode('UTF-8')
    #
    #                     # data = pickle.dumps(row)
    #
    #                     # izzz.write(data)
    #
    #                     izzz.write(jd)
    #                     # print('7777', len(jd))
    #                     # print('rc', self.rc)
    #
    #
    #     except pd.errors.EmptyDataError:
    #         print('dfdf')
    #     except:
    #         print(traceback.print_exc())

    def On_readyRead1(self):
        try:
            st = time.time()
            print('ssss')


            df1=pd.read_csv(self.path,header=None,dtype={20:str},skiprows=self.notisSNo)
            self.notisSNo += df1.shape[0]

            df1 = df1.iloc[:, [2, 4, 5, 6, 7, 12, 20, 23, 25, 26, 27, 28]]
            #
            df1.columns = ['TradeNo', 'Token', 'Qty', 'Price', 'BuySell', 'ClientID', 'TerminalID', 'Symbol',
                           'Instrument',
                           'EXP', 'Strike', 'OPTType']

            print(self.notisSNo,df1.shape)

            df1 = df1.drop_duplicates()

            tradesDict = dict(zip(df1['TradeNo'], df1['ClientID']))

            common_keys = set(self.clientDict.keys()) & set(tradesDict.keys())

            self.clientDict.update(tradesDict)




            df = df1[~df1['TradeNo'].isin(common_keys)]
            print('common_keys',common_keys,df1.shape,df.shape)

            if df.shape[0]!=0:

            #     df1 = df.iloc[:, [2, 4, 5, 6, 7, 12, 20, 23, 25, 26, 27, 28]]
            # #
            #     df1.columns = ['TradeNo', 'Token', 'Qty', 'Price', 'BuySell', 'ClientID', 'TerminalID', 'Symbol',
            #                    'Instrument',
            #                    'EXP', 'Strike', 'OPTType']



                self.rc += df.shape[0]
                print('rc', self.rc)

                convert_dict = {'TerminalID': str, 'ClientID': str}

                df = df.astype(convert_dict)

                # print(df.dtypes)

                # df['TerminalID'] = df['TerminalID'].astype(str)

                df["Qty"] = np.where(df['BuySell'] == 1, df['Qty'], -df['Qty'])
                df["TerminalID"] = np.where(df['TerminalID'] == '0', df['ClientID'], df['TerminalID'])
                df["Price"] = df["Price"] / 100
                df["Strike"] = df["Strike"] / 100
                df["EXP"] = df["EXP"].apply(self.updateexp)


                df["Tradeamt"] = -df["Qty"] * df["Price"]
                df["Val"] = abs(df["Tradeamt"]) / 10000000



                df["TOC"] = np.where(df['OPTType'] == 'XX', np.where(df['BuySell'] == 1, (df['Val'] * self.f_buyex),
                                                                     (df['Val'] * self.f_sellex)),
                                     np.where(df['BuySell'] == 1, (df['Val'] * self.o_buyex),
                                              (df['Val'] * self.o_sellex)))

                # df.to_csv('ddd.csv')
                # print(df["Strike"].dtype)


                # print(df["Qty"])
                # print(df["Tradeamt"])
                # print(df.columns)

                # df=df.groupby(['TerminalID','Token','EXP','Strike','OPTType','Symbol','Instrument'])["Qty","Tradeamt"].sum().reset_index()
                df = df.groupby(['TerminalID', 'Token', 'EXP', 'Strike', 'OPTType', 'Symbol', 'Instrument']).aggregate(
                    {'Qty': 'sum', 'Tradeamt': 'sum', 'TOC': 'sum'}).reset_index()

                for row in df.values.tolist():
                    # print('kfjdkfj')

                    for izzz in self.clientList:
                        row = str(row)

                        jd = row.encode('UTF-8')

                        size = len(jd)

                        if (size > 200):
                            print('size', size)


                        blank = 200 - size
                        k = row + (blank * ' ')
                        #
                        jd = k.encode('UTF-8')

                        # data = pickle.dumps(row)

                        # izzz.write(data)

                        izzz.write(jd)
                        # print('7777', len(jd))
                        # print('rc', self.rc)







            ##################################################################################################
            # print(df)




            # self.maindf = pd.concat([self.maindf, df1],
            #                         axis=0,ignore_index=True)
            # no=self.notisSNo
            #
            # self.notisSNo = self.maindf.shape[0]
            # df1 = self.maindf
            #
            # if df1.shape[0] != 0:
            #
            #
            #     df1 = df1.iloc[:, [2, 4, 5, 6, 7, 12, 20, 23, 25, 26, 27, 28]]
            #
            #     df1.columns = ['TradeNo', 'Token', 'Qty', 'Price', 'BuySell', 'ClientID', 'TerminalID', 'Symbol',
            #                    'Instrument',
            #                    'EXP', 'Strike', 'OPTType']
            #     # print('dfj',df.shape)
            #     df = df1.drop_duplicates()
            #
            #     df = df.loc[no:, :]
            #
            #
            #     self.rc += df.shape[0]
            #     print('rc', self.rc)
            #
            #     convert_dict = {'TerminalID': str, 'ClientID': str}
            #
            #     df = df.astype(convert_dict)
            #
            #     # print(df.dtypes)
            #
            #     # df['TerminalID'] = df['TerminalID'].astype(str)
            #
            #     df["Qty"] = np.where(df['BuySell'] == 1, df['Qty'], -df['Qty'])
            #     df["TerminalID"] = np.where(df['TerminalID'] == '0', df['ClientID'], df['TerminalID'])
            #     df["Price"] = df["Price"] / 100
            #     df["Strike"] = df["Strike"] / 100
            #     df["EXP"] = df["EXP"].apply(self.updateexp)
            #
            #
            #     df["Tradeamt"] = -df["Qty"] * df["Price"]
            #     df["Val"] = abs(df["Tradeamt"]) / 10000000
            #
            #
            #
            #     df["TOC"] = np.where(df['OPTType'] == 'XX', np.where(df['BuySell'] == 1, (df['Val'] * self.f_buyex),
            #                                                          (df['Val'] * self.f_sellex)),
            #                          np.where(df['BuySell'] == 1, (df['Val'] * self.o_buyex),
            #                                   (df['Val'] * self.o_sellex)))
            #
            #     # df.to_csv('ddd.csv')
            #     # print(df["Strike"].dtype)
            #
            #
            #     # print(df["Qty"])
            #     # print(df["Tradeamt"])
            #     # print(df.columns)
            #
            #     # df=df.groupby(['TerminalID','Token','EXP','Strike','OPTType','Symbol','Instrument'])["Qty","Tradeamt"].sum().reset_index()
            #     df = df.groupby(['TerminalID', 'Token', 'EXP', 'Strike', 'OPTType', 'Symbol', 'Instrument']).aggregate(
            #         {'Qty': 'sum', 'Tradeamt': 'sum', 'TOC': 'sum'}).reset_index()
            #
            #     for row in df.values.tolist():
            #         # print('kfjdkfj')
            #
            #         for izzz in self.clientList:
            #             row = str(row)
            #
            #             jd = row.encode('UTF-8')
            #
            #             size = len(jd)
            #
            #
            #
            #
            #
            #             if (size > 200):
            #                 print('size', size)
            #
            #
            #             blank = 200 - size
            #             k = row + (blank * ' ')
            #             #
            #             jd = k.encode('UTF-8')
            #
            #             # data = pickle.dumps(row)
            #
            #             # izzz.write(data)
            #
            #             izzz.write(jd)
            #             # print('7777', len(jd))
            #             # print('rc', self.rc)
            et = time.time()
            print('et', et - st)

        except pd.errors.EmptyDataError:
            print('dfdf')
        except:
            print(traceback.print_exc())




    def updateexp(self,exp):
        # exp1 = datetime.datetime.strptime(exp, '%d %b %Y').strftime('%Y%m%d')
        exp = datetime.datetime.fromtimestamp(exp)
        exp = exp.replace(2024)
        exp = datetime.datetime.strftime(exp, '%Y%m%d')

        return exp


    def tradeFiletoNumpy(self):

        i=''

        st=time.time()
        path = r'\\192.168.102.102\TradeAPI\TradeFO_'+self.today+'.txt'
        # path = r'\\192.168.102.102\TradeAPI\TradeFO_24032023.txt'

        f=open(path,'r')

        # conn = sqlite3.connect(DBFilePath)
        # lines = f.readlines()[self.rc:self.rc+500]
        lines = f.readlines()[self.rc:]

        # print(lines)
        # di = {}
        # # print("di[finalkey]:",di['ee'])
        # # di['ee'] = True
        # # print("111di[finalkey]:", di['ee'])
        if (lines != []):
            # print(lines)
            # newLines = lines[self.rc:]

            for k in lines:
                # print(k)
                i = k.split(',')
                # print(len(i))
                Clicode = i[12]
                ctcl = i[20]
                neatid = i[10]
                TradeNo=i[2]

                # print('ppp',Clicode,neatid,ctcl)
                if(Clicode == 'A1946' and neatid == '35788' and ctcl == '111111111111122' ):

                    # print('ddd',k)
                    # print(type(k))
                    k=k.replace("111111111111122","A1946")
                    # print('ddf',k)
                ada = str(i[7])
                sda = ada.replace(' ', '')
                acc = i[20]
                if (acc == '0'):
                    amm = i[10]
                else:
                    amm = acc

                if (acc == '111111111111122' and 'A2221' in i[12]):
                    # print(i[17]+'b')
                    amm = 'TM2221'
                if (acc == '111111111111122' and 'A1946' in i[12]):
                    # print(i[17]+'b')
                    amm = 'TM46'
                if (acc == '111111111111122' and 'A1675' in i[12]):
                    # print(i[17]+'b')
                    amm = 'TM75'
                if (acc == '111111111111122' and 'A1676' in i[12]):
                    # print(i[17]+'b')
                    amm = 'TM76'

                jk = i[7]

                if (jk == '1'):
                    jk = 1
                else:
                    jk = -1

                # i=(i[0], i[1], i[2], i[3], i[4], i[5], i[6], sda, i[8], i[9], i[10], i[11], i[12], jk, i[14], i[15], i[16], i[17], i[18], i[19], i[20], i[21], i[22], i[23], i[24], i[25], amm)

                # cursor.execute('INSERT INTO all_trade_fo(tradeid, sement, instrument_type, symbol, exp, strike_price, option_type, instrument, other_2, ord_type, other_3, neetid, other4, bs, qty, price, bs_1, client_code, member_code, ord_type_2, ord_type_3, ord_time_1, ord_time_2, ordid, membercode, tradetime, ctcl) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                # i[0], i[1], i[2], i[3], i[4], i[5], i[6], sda, i[8], i[9], i[10], i[11], i[12], jk, i[14], i[15], i[16], str(i[17]).replace(' ', ''), i[18], i[19], i[20], i[21], i[22], i[23], i[24], i[25], amm)
                self.rc+=1
                try:
                    # conn.execute(
                    #     'INSERT INTO AllTradeFO(tradeid, sement, nToken,instrument_type, symbol, exp, strike_price, option_type, instrument, other_2, ord_type, other_3, neetid, other4, bs, qty, price, '
                    #     'bs_1, client_code, member_code, ord_type_2, ord_type_3, ord_time_1, ord_time_2, ordid, membercode, tradetime, ctcl) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    #     (i[2], '1', int(i[4]), i[25], i[23], i[26], i[27], i[28], '', '', '', '', i[10],
                    #     '', jk, i[5], float(i[6]) / 100, i[11], str(i[12]).replace(' ', ''), i[13], '', '',
                    #     i[3], i[3], i[8], i[13], i[17], amm))
                    tradeid=str(i[2])
                    clientid=str(i[12]).replace(' ', '')

                    finalkey=tradeid +'-' +clientid


                    # if (r.get(finalkey) ==1):
                    #     print('exist')
                    # else:
                    # r.set(,1)
                    if(di.get(finalkey) == None):
                        di[finalkey] = True
                        self.fc = self.fc + 1
                        print("fc",self.fc)
                        for izzz in self.clientList:
                            jd = k.encode('UTF-8')
                            size = len(jd)
                            # print('size',size)
                            if (size > 200):
                                print('size', size)

                            blank = 200 - size
                            k = k + (blank * ' ')

                            jd = k.encode('UTF-8')
                            izzz.write(jd)
                            print('7777', len(jd))
                            print('rc', self.rc)



                    # conn.commit()


                except:
                    print('error')
                    print(traceback.print_exc())






                    # self.clientConnection.waitForBytesWritten(5000)
                    # time.sleep(0.005)


                # self.alltradearr = np.vstack((self.alltradearr, abc))
                # self.alltradearr[self.rc,:]=abc
            # conn.commit()
        # time.sleep(30)




        # print(self.alltradearr)
        print('rc,fc',self.rc,self.fc)
        et = time.time()
        print('time',et-st)
        # self.arr = self.alltradearr[:self.rc,:].tolist()
        # print(sys.getsizeof(self.arr))



    def dealCommunication(self):
        # Get a QTcpSocket from the QTcpServer

        self.clientConnection = self.tcpServer.nextPendingConnection()

        while True:
            try:
                self.clientConnection.waitForReadyRead()
                data = self.clientConnection.read(1024)

                if not data:
                    break

                # d=data.decode()
                # data=json.loads(d)
                data=pickle.loads(data)
                print(data)

                if(data['Type']=='Auth'):
                    if(data['User']=='Arham'):
                        self.status='Connected'
                        print('connected')
                    else:
                        self.status='Invalid User'

                    dict = {'Type': 'AuthRes', 'status':self.status }
                    # jd = json.dumps(dict)
                    jd = pickle.dumps(dict)
                    self.clientConnection.write(jd)

                    if(self.status=='Invalid User'):

                        self.clientConnection.disconnectFromHost()
                    else:
                        self.clientList.append(self.clientConnection)



                elif(data['Type']=='sendTrade'):
                    self.On_readyRead1()
                    self.timer.start()
                #     try:
                #         print(len(self.arr))
                #         strt=0
                #         while(strt<11):
                #             end=strt+5
                #             print(strt,end)
                #             dict = {'Type': 'Trade', 'data': self.arr[strt:end+1]}
                #             print(dict)
                #             jd = pickle.dumps(dict)
                #             self.clientConnection.write(jd)
                #             time.sleep(1)
                #             print('hello')
                #             strt=end
                #     except:
                #         print(traceback.print_exc())
                #     # for i in range(3):
                #     #     dict = {'Type': 'Trade', 'data': self.arr[:5 + 1]}
                #     #     print(dict)
                #     #     # jd = json.dumps(dict)
                #     #     jd = pickle.dumps(dict)
                #     #     self.clientConnection.write(jd)
                #     #     time.sleep(1)
                #



            except:
                print(traceback.print_exc())


        # now disconnect connection.
        # print('disconnected')

if __name__ == '__main__':
    app = QApplication(sys.argv)
    server = Server()
    # server.sessionOpened()
    sys.exit(app.exec_())





















# from PyQt5.QtCore import QObject,pyqtSignal,pyqtSlot
# from PyQt5.QtNetwork import *
#
# import time
# import struct
# import sys
# import traceback
# from PyQt5.QtWidgets import *
#
#
#
# class TradeSocket(QObject):
#     def __init__(self):
#         super(TradeSocket, self).__init__()
#
#         self.server=QTcpServer()
#         # self.server.
#         self.port = 7011
#
#
#
#         self.server.listen(233.1.5.2, self.port)
#         self.server.newConnection.connect(self.on_newConnection)
#
#
#
#
#
#
#
#
#
#
#
#
#
#     def on_newConnection(self):
#         while self.server.hasPendingConnections():
#             print("Incoming Connection...")
#
#     def __onDisconnected(self):
#         print('Disconnected')
#
#
# if __name__ == "__main__":
#
#
#
#     obj = TradeSocket()
#
#


