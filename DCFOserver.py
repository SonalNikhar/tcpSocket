import datetime
import numpy as np
import time
import datatable as dt
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



class Server(QMainWindow):
    def __init__(self):
        super().__init__()
        self.rc = 0
        self.len=0
        self.DCSNo=0
        self.Expense()

        self.tcpServer = QTcpServer(self)
        PORT = 22777
        address = QHostAddress('192.168.130.42')

        # self.alltradearr = np.empty((1000000, 17), dtype=object)

        self.today = datetime.datetime.today().strftime('%Y%m%d')
        self.path = r'\\192.168.102.112\Shared\OnlineTrades\\%s\\%s.txt' % (self.today, self.today)
        # self.path = r'\\192.168.102.112\Shared\OnlineTrades\\20230428\\20230428.txt'

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

        self.f_buyex = self.Expense[:, 1].sum()
        self.f_sellex = self.Expense[:, 2].sum()
        self.o_buyex = self.Expense[:, 3].sum()
        self.o_sellex = self.Expense[:, 4].sum()
    def On_readyRead(self):
        try:
            st=time.time()
            df=pd.read_csv(self.path,header=None,skiprows=self.DCSNo,dtype={4:str})


            if df.shape[0]!=0:

                self.DCSNo+=df.shape[0]
                # print(df.columns)


                df.columns = ['t1', 'Exch', 'Token', 'Symbol', 'EXP', 'Strike', 'OptType', 'Instrument',
                                 'Qty', 'Price','ClentID','OPNo','BuySell','TradeNo']


                convert_dict = {'EXP': str,'ClentID':str
                                }

                df = df.astype(convert_dict)

                # df.drop_duplicates(subset=['TradeNo','ClientID'])
                # print(df.dtypes)

                # df['TerminalID'] = df['TerminalID'].astype(str)


                df["Qty"]=np.where(df['BuySell']==1, df['Qty'], -df['Qty'])




                # float(int(data[6]) / 100) * -TQty

                df["Tradeamt"]=-df["Qty"] * df["Price"]

                df["Val"] = abs(df["Tradeamt"]) / 10000000


                df["TOC"] = np.where(df['OptType']=='XX', np.where(df['BuySell']==1,(df['Val'] * self.f_buyex), (df['Val'] * self.f_sellex)),np.where(df['BuySell']==1,(df['Val'] * self.o_buyex), (df['Val'] * self.o_sellex)))

                # print(df["Qty"])
                # print(df["Tradeamt"])
                # print(df.columns)


                # df=df.groupby(['TerminalID','Token','EXP','Strike','OPTType','Symbol','Instrument'])["Qty","Tradeamt"].sum().reset_index()
                df=df.groupby(['ClentID','Token','EXP','Strike','OptType','Symbol','Instrument']).aggregate({'Qty':'sum','Tradeamt':'sum','TOC':'sum'}).reset_index()
                # for i in df.to_numpy():
                #     # print(type(row))
                    # self.sgdatasend.emit(i)
                et=time.time()
                print('et',et-st)

                # df.to_csv('d:/aaa.csv')
                # print('fjdf')
                for row in df.values.tolist():
                    for izzz in self.clientList:
                        row=str(row)
                        # print(row)
                        jd = row.encode('UTF-8')
                        # print('djf',jd)
                        size = len(jd)



                        # print('size',size)
                        if (size > 200):
                            print('size', size)
                        #
                        blank = 200 - size
                        k = row + (blank * ' ')
                        #
                        jd = k.encode('UTF-8')

                        # data = pickle.dumps(row)

                        # izzz.write(data)

                        izzz.write(jd)
                        # print('7777', len(jd))
                        # print('rc', self.rc)

        except pd.errors.EmptyDataError:
            print('dfdf')
        except:
            print(traceback.print_exc())

    def tradeFiletoNumpy(self):

        i=''
        st=time.time()
        path = r'\\192.168.102.222\Shared\OnlineTrades\\%s\\%s.txt' %(self.today,self.today)

        # path = r'\\192.168.102.222\Shared\OnlineTrades\20230210\20230210.txt'

        f=open(path,'r')



        lines = f.readlines()[self.rc:]

        # print(lines)
        if (lines != []):
            # print(lines)
            # newLines = lines[self.rc:]

            for k in lines:
                print(k)
                # i = k.split(',')
                # print(len(i))
                # Clicode = int(i[0])
                # token = int(i[2])
                # dQty = int(i[8])
                # damt = float(i[9])
                # sym = i[3].replace(' ', '')
                # exp = i[4]
                #
                # strike = format(float(i[5]), '.2f')
                # opt = i[6]
                # ins=i[7]

                # abc = dt.Frame([i[11],
                #                 ['NSEFO'], [i[2]], [i[14]], [i[15]], [0], [0], [0], [0], [0], [0], [0] ]).to_numpy()
                #
                # abc = np.asarray([[Clicode],
                #                 ['NSEFO'], [token],[ins], [sym], [exp], [strike], [opt], [dQty], [damt], [0], [0],[0], [0],[0],[dQty],[damt]])
                # self.alltradearr[self.rc,:]=[Clicode,
                #                              'NSEFO', token,ins, sym, exp,
                #                              strike, opt, dQty, damt, 0,
                #                              0,0, 0,0,dQty,damt]


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
                    print('djkfj')
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
