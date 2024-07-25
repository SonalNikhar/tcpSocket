import datetime
import numpy as np
import time
import datatable as dt
import sys
import traceback
import json
import pickle

from PyQt5.QtCore import QByteArray, QDataStream, QIODevice
from PyQt5.QtWidgets import QApplication, QDialog,QMainWindow
from PyQt5.QtNetwork import QHostAddress, QTcpServer
from PyQt5.QtWidgets import  *
from PyQt5.QtCore import *
from pymongo import MongoClient
import pandas as pd



class Server(QMainWindow):
    def __init__(self):
        super().__init__()
        self.rc = 0
        self.len=0
        self.DCSNo=0
        self.Expense()
        self.tcpServer = QTcpServer(self)
        PORT = 22999
        address = QHostAddress('192.168.130.42')

        # self.alltradearr = np.empty((1000000, 17), dtype=object)

        self.today = datetime.datetime.today().strftime('%Y%m%d')
        self.path = r'\\192.168.102.112\Shared\OnlineTrades\\%s\\%sCM.txt' %(self.today,self.today)
        # self.path = r'\\192.168.102.112\Shared\OnlineTrades\20230428\20230428CM.txt'

        self.alltradearr = np.empty((100000, 13), dtype=object)

        self.clientList = []
        self.tcpServer.newConnection.connect(self.dealCommunication)
        self.timer=QTimer()
        self.timer.setInterval(3000)
        self.timer.timeout.connect(self.On_readyRead)
        # self.timer.start()
        # self.tradeFiletoNumpy()
        self.tcpServer.listen(address, PORT)
        self.tcpServer.waitForNewConnection(1000000)


    def Expense(self):
        mongoclient = MongoClient("192.168.130.42", 27017)
        Calculation_data = mongoclient['Calculated_data']
        Expense_collection = Calculation_data['Expense']
        self.dbExpense = list(Expense_collection.find())
        self.Expense = pd.DataFrame(self.dbExpense)[
            ['particulars', 'f_buy', 'f_sell', 'o_buy', 'o_sell', 'i_buy', 'i_sell', 'd_buy', 'd_sell']].values

        self.i_buyex = self.Expense[:, 5].sum()
        self.i_sellex = self.Expense[:, 6].sum()
        self.d_buyex = self.Expense[:, 7].sum()
        self.d_sellex = self.Expense[:, 8].sum()


    def On_readyRead(self):
        try:
            # q '], data['Buy_Sell'], data['Series'], data['tradeNO']

            st = time.time()
            df = pd.read_csv(self.path, header=None, skiprows=self.DCSNo)

            if df.shape[0] != 0:

                self.DCSNo += df.shape[0]
                # print(df.columns)
                # print(df.dtypes)
                # df = df.iloc[:, [2, 4, 5, 6, 7,12, 20,23, 25, 26, 27, 28]]

                df.columns = ['t1', 'Exch', 'ClentID', 'Symbol',
                              'Qty', 'Price', 'Opt_Orderno', 'BuySell', 'Series', 'TradeNo']

                # df.drop_duplicates(subset=['TradeNo','ClientID'])
                # print(df.dtypes)

                # df['TerminalID'] = df['TerminalID'].astype(str)

                convert_dict = { 'ClentID': str
                                }

                df = df.astype(convert_dict)

                df["Qty"] = np.where(df['BuySell'] == 1, df['Qty'], -df['Qty'])

                # float(int(data[6]) / 100) * -TQty

                df["Tradeamt"] = -df["Qty"] * df["Price"]

                df = df.groupby(['ClentID', 'Series', 'Symbol', 'BuySell']).aggregate(
                    {'Qty': 'sum', 'Tradeamt': 'sum', 'Price': 'mean'}).reset_index()

                df["BQty"] = np.where(df['BuySell'] == 1, df['Qty'], 0.0)
                df["SQty"] = np.where(df['BuySell'] == 2, df['Qty'], 0.0)

                df["BAmt"] = np.where(df['BuySell'] == 1, df['Tradeamt'], 0.0)
                df["SAmt"] = np.where(df['BuySell'] == 2, df['Tradeamt'], 0.0)

                # df.to_csv('d:/aba.csv')

                df = df.groupby(['ClentID', 'Series', 'Symbol']).aggregate(
                    {'Qty': 'sum', 'Tradeamt': 'sum', 'Price': 'mean', 'BQty': 'sum', 'SQty': 'sum', 'BAmt': 'sum',
                     'SAmt': 'sum'}).reset_index()

                df["IntraDayQty"] = np.where(abs(df['BQty']) < abs(df['SQty']), abs(df['BQty']), abs(df['SQty']))

                df["BuyIntra"] = (abs(df["IntraDayQty"] * df["Price"]) / 10000000) * self.i_buyex
                df["SellIntra"] = (abs(df["IntraDayQty"] * df["Price"]) / 10000000) * self.i_sellex

                df["TotalIntra"] = df["BuyIntra"] + df["SellIntra"]

                df.iloc[:,"fjdfj"]="gdfgdfg"

                # for i in df.to_numpy():
                #     # print(type(row))
                # self.sgdatasend.emit(i)
                et = time.time()
                print('et', et - st)

                # df.to_csv('d:/aaa.csv')
                # print('fjdf')
                for row in df.values.tolist():
                    for izzz in self.clientList:
                        row = str(row)
                        jd = row.encode('UTF-8')
                        size = len(jd)

                        blank = 200 - size
                        k = row + (blank * ' ')

                        jd = k.encode('UTF-8')
                        izzz.write(jd)
                        print(len(jd))
                        print(self.rc)

                        # self.clientConnection.waitForBytesWritten(5000)
                        # time.sleep(0.005)

        except pd.errors.EmptyDataError:
            print('dfdf')
        except:
            print(traceback.print_exc())

    def tradeFiletoNumpy(self):

        i=''
        st=time.time()
        path = r'\\192.168.102.222\Shared\OnlineTrades\\%s\\%sCM.txt' %(self.today,self.today)

        # path = r'\\192.168.102.222\Shared\OnlineTrades\20221202\20221202CM.txt'

        f=open(path,'r')



        lines = f.readlines()[self.rc:]

        # print(lines)
        if (lines != []):
            # print(lines)
            # newLines = lines[self.rc:]

            for k in lines:
                print(k)

                # i=i +k

                # self.alltradearr[self.rc, :]=i
                self.rc += 1


                for izzz in self.clientList:
                    jd = k.encode('UTF-8')
                    size=len(jd)

                    blank=100-size
                    k=k+ (blank* ' ')

                    jd = k.encode('UTF-8')
                    izzz.write(jd)
                    print(len(jd))
                    print(self.rc)

                    # self.clientConnection.waitForBytesWritten(5000)
                    # time.sleep(0.005)


                # self.alltradearr = np.vstack((self.alltradearr, abc))
                # self.alltradearr[self.rc,:]=abc





        # print(self.alltradearr)
        print('rows',self.rc)
        et = time.time()
        print('time',et-st)
        self.arr = self.alltradearr[:self.rc,:].tolist()
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
                    self.On_readyRead()
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
