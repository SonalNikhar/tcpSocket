import datetime
import pandas as pd
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



class Server(QMainWindow):
    def __init__(self):
        super().__init__()
        self.rc = 0
        self.len=0
        self.notisSNo=0
        self.maindf=pd.DataFrame()


        self.Expense()
        self.tcpServer = QTcpServer(self)
        PORT = 25555
        # PORT = 8888
        address = QHostAddress('192.168.130.42')
        self.tcpServer.listen(address, PORT)
        # self.alltradearr = np.empty((1000000, 17), dtype=object)

        self.alltradearr = np.empty((100000, 13), dtype=object)
        self.today = datetime.datetime.today().strftime('%d%m%Y')
        self.path = r'\\192.168.102.102\Techexcel\TradeAPI\TradeCM_' + self.today + '.txt'
        # self.path = r'\\192.168.102.102\TradeAPI\TradeCM_28042023.txt'

        self.clientList = []
        self.tcpServer.newConnection.connect(self.dealCommunication)
        self.timer=QTimer()
        self.timer.setInterval(10000)
        self.timer.timeout.connect(self.On_readyRead)


        # self.timer.start()
        # self.tradeFiletoNumpy()

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
            st=time.time()
            df1=pd.read_csv(self.path,header=None,skiprows=self.notisSNo,dtype={23:str})

            self.maindf = pd.concat([self.maindf, df1],
                                    axis=0, ignore_index=True)

            df1 = self.maindf

            if df1.shape[0]!=0:


                # print(df.columns)
                # print(df.dtypes)
                df1 = df1.iloc[:, [0, 4, 5, 6, 7,12,23,26, 27]]

                df1.columns = ['TradeNo', 'Token', 'Qty', 'Price', 'BuySell', 'ClientID', 'TerminalID', 'Symbol', 'Series',
                                 ]

                # df=df.drop_duplicates(subset=['TradeNo','ClientID'])
                df=df1.drop_duplicates()

                df = df.loc[self.notisSNo:, :]

                self.notisSNo = self.maindf.shape[0]

                # pd.merge(contract, span1, how='left', left_on=['symbol', 'exp', 'strk1', 'opt'],
                #          right_on=['symbol', 'exp', 'strk', 'opt']).to_numpy()

                # df2 = df
                # print(df.shape)
                self.rc += df.shape[0]
                print('rc', self.rc)

                convert_dict = {'TerminalID': str, 'ClientID': str
                                }

                df = df.astype(convert_dict)



                # print(df.dtypes)

                # df['TerminalID'] = df['TerminalID'].astype(str)


                df["Qty"]=np.where(df['BuySell']==1, df['Qty'], -df['Qty'])
                df["TerminalID"]=np.where(df['TerminalID']=='0', df['ClientID'], df['TerminalID'])
                df["Price"]= df["Price"] / 100

                # float(int(data[6]) / 100) * -TQty

                df["Tradeamt"]=-df["Qty"] * df["Price"]




                # df["TOC"] = np.where(df['OPTType']=='XX', np.where(df['BuySell']==1,(df['Val'] * self.f_buyex), (df['Val'] * self.f_sellex)),np.where(df['BuySell']==1,(df['Val'] * self.o_buyex), (df['Val'] * self.o_sellex)))

                # print(df["Qty"])
                # print(df["Tradeamt"])
                # print(df.columns)


                # df=df.groupby(['TerminalID','Token','EXP','Strike','OPTType','Symbol','Instrument'])["Qty","Tradeamt"].sum().reset_index()
                df=df.groupby(['TerminalID','Token','Series','Symbol','BuySell']).aggregate({'Qty':'sum','Tradeamt':'sum','Price':'mean'}).reset_index()

                df["BQty"] = np.where(df['BuySell'] == 1, df['Qty'], 0.0)
                df["SQty"] = np.where(df['BuySell'] == 2, df['Qty'], 0.0)

                df["BAmt"] = np.where(df['BuySell'] == 1, df['Tradeamt'], 0.0)
                df["SAmt"] = np.where(df['BuySell'] == 2, df['Tradeamt'], 0.0)






                # df.to_csv('d:/aba.csv')

                df = df.groupby(['TerminalID', 'Token', 'Series', 'Symbol']).aggregate(
                    {'Qty': 'sum', 'Tradeamt': 'sum','Price':'mean', 'BQty': 'sum', 'SQty': 'sum','BAmt': 'sum', 'SAmt': 'sum'}).reset_index()

                df["IntraDayQty"] = np.where(abs(df['BQty']) < abs(df['SQty']), abs(df['BQty']), abs(df['SQty']))

                df["BuyIntra"] = (abs(df["IntraDayQty"] * df["Price"])/ 10000000) * self.i_buyex
                df["SellIntra"] = (abs(df["IntraDayQty"] * df["Price"])/ 10000000) * self.i_sellex

                df["TotalIntra"] = df["BuyIntra"] + df["SellIntra"]

                # df.groupby(['TerminalID','Token','Series','Symbol'])['Qty'].apply(
                #     lambda s: pd.Series(s.nlargest(2).values, index=['BQty', 'SQty'])
                # ).unstack()
                # for i in df.to_numpy():
                #     # print(type(row))
                    # self.sgdatasend.emit(i)
                et=time.time()
                print('et',et-st)

                # df.to_csv('d:/aaa.csv')
                # print('fjdf')


                for row in df.values.tolist():
                    for izzz in self.clientList:
                        row = str(row)
                        # print(row)
                        jd = row.encode('UTF-8')
                        # print('djf',jd)
                        size = len(jd)
                        if (size > 220):
                            print('size', size)

                        blank = 220 - size
                        k = row + (blank * ' ')

                        jd = k.encode('UTF-8')
                        izzz.write(jd)
                        # print(len(jd))
                        # print(self.rc)

                        # self.clientConnection.waitForBytesWritten(5000)
                        # time.sleep(0.005)

        except pd.errors.EmptyDataError:
            print('dfdf')
        except:
            print(traceback.print_exc())
    def tradeFiletoNumpy(self):

        i=''

        st=time.time()
        path = r'\\192.168.102.102\TradeAPI\TradeCM_'+self.today+'.txt'
        # path = r'\\192.168.102.102\TradeAPI\TradeCM_24032023.txt'
        #
        f=open(path,'r')



        lines = f.readlines()[self.rc:]

        # print(lines)
        if (lines != []):
            # print(lines)
            # newLines = lines[self.rc:]

            for k in lines:
                print(k)
                i = k.split(',')
                # print(len(i))
                Clicode = i[12]
                ctcl = i[20]
                neatid = i[10]

                # print('ppp',Clicode,neatid,ctcl)
                if(Clicode == 'A1946' and neatid == '35788' and ctcl == '111111111111122' ):

                    # print('ddd',k)
                    # print(type(k))
                    k=k.replace("111111111111122","A1946")


                # self.alltradearr[self.rc, :]=i
                self.rc += 1


                for izzz in self.clientList:
                    jd = k.encode('UTF-8')
                    size=len(jd)
                    # print('size',size)
                    if(size>220):
                        print('size',size)

                    blank=220-size
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
