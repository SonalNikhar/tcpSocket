import websocket
import pandas as pd
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



class Server(QMainWindow):
    def __init__(self):
        super().__init__()
        self.rcnotis = 0
        self.rcDC = 0
        self.len=0

        self.notisSNo=0
        self.DCSno=0

        # self.tcpServer = QTcpServer(self)
        # PORT = 8080
        # address = QHostAddress('192.168.102.169')
        # # PORT = 31698
        # # address = QHostAddress('127.0.0.9')
        # self.tcpServer.listen(address, PORT)
        # # self.alltradearr = np.empty((1000000, 17), dtype=object)
        #
        # self.alltradearr = np.empty((100000, 13), dtype=object)
        self.today = datetime.datetime.today().strftime('%d%m%Y')

        self.today1 = datetime.datetime.today().strftime('%Y%m%d')
        self.DCpath = r'\\192.168.102.112\Shared\OnlineTrades\%s\%s.txt' % (self.today1, self.today1)
        # self.DCpath = r'\\192.168.102.112\Shared\OnlineTrades\20230428\20230428.txt'
        self.notispath = r'\\192.168.102.102\TechExcel\TradeAPI\TradeFO_' + self.today + '.txt'
        # self.notispath = path = r'\\192.168.102.102\TechExcel\TradeAPI\TradeFO_28042023.txt'
        #
        # self.clientList = []
        # self.tcpServer.newConnection.connect(self.dealCommunication)
        self.timer=QTimer()
        self.timer.setInterval(2000)
        self.timer.timeout.connect(self.On_readyRead)
        self.timer.start()
        # self.tradeFiletoNumpy()



    def On_readyRead(self):
        try:
            st=time.time()
            DCdf = pd.read_csv(self.DCpath, header=None)
            self.DCSno += DCdf.shape[0]

            notisdf=pd.read_csv(self.notispath,header=None,dtype={20:str})
            self.notisSNo += notisdf.shape[0]


            # notisdf = pd.read_csv(self.notispath, header=None, dtype={20: str}, skiprows=self.notisSNo)
            # #
            # self.maindf = pd.concat([self.maindf, notisdf],
            #                         axis=0, ignore_index=True)
            # no = self.notisSNo
            # #
            # self.notisSNo = self.maindf.shape[0]
            # #
            # df1 = self.maindf


            # df = notisdf.iloc[:, [2,12]]
            notisdf = notisdf.iloc[:, [2, 4, 5, 6, 7, 12, 23, 25, 26, 27, 28]]

            notisdf.columns = ['TradeNo', 'Token', 'Qty', 'Price', 'BuySell', 'ClientID', 'Symbol',
                          'Instrument','EXP', 'Strike', 'OPTType']

            convert_dict = { 'ClientID': str
                            }

            notisdf = notisdf.astype(convert_dict)

            # notisdf.drop_duplicates(subset=['TradeNo', 'ClientID'])

            notisdf=notisdf.drop_duplicates()

            # notisdf.to_csv('d:/dc33file.csv')

            # notisdf["Qty"] = np.where(notisdf['BuySell'] == 1, notisdf['Qty'], -notisdf['Qty'])
            # notisdf["TerminalID"] = np.where(notisdf['TerminalID'] == '0', notisdf['ClientID'], notisdf['TerminalID'])
            notisdf["Price"] = notisdf["Price"] / 100
            notisdf["Strike"] = notisdf["Strike"] / 100
            notisdf["EXP"] = notisdf["EXP"].apply(self.updateexp)

            # 'TradeNo', 'Token', 'Qty', 'Price', 'BuySell', 'ClientID', 'Symbol',
            # 'Instrument',
            # 'EXP', 'Strike', 'OPTType'
            #                     [13,2,8,9,12,10,7,4,5,6]
            # DCdf = DCdf.iloc[:, [2,3, 4, 5, 6, 7, 8,9,10,12,13]]
            # DCdf.columns = [  'Token' , 'Symbol','EXP', 'Strike', 'OPTType','Instrument', 'Qty', 'Price','ClientID','BuySell'
            #               ,'TradeNo']


            DCdf = DCdf.iloc[:, [13,2,8,9,12,10,3,7,4,5,6]]

            DCdf.columns = ['TradeNo', 'Token', 'Qty', 'Price', 'BuySell', 'ClientID', 'Symbol',
            'Instrument',
            'EXP', 'Strike', 'OPTType']

            convert_dict = {'ClientID': str
                            }

            DCdf = DCdf.astype(convert_dict)




            # notisdf.to_csv('d:\Fnotis.csv')
            # DCdf.to_csv('d:\DC.csv')

            # print('dfj',notisdf.dtypes,DCdf.dtypes)

            i1 = pd.MultiIndex.from_frame(notisdf)
            i2 = pd.MultiIndex.from_frame(DCdf)
            # # print(i1)
            # # print(i2)
            dd=notisdf[~i1.isin(i2)]
            dd.to_csv('DCNew.csv')

            # ff=pd.concat([notisdf,DCdf])
            # ff1=ff.drop_duplicates(subset=['TradeNo', 'ClientID'],keep=False)
            #
            self.ffff = open(self.DCpath, 'a+')
            # self.ffff = open('dccopy', 'a+')
            #
            for index, data in dd.iterrows():
                # print(row["Name"], row["Age"])
                trd_string = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
                    0, 'NSEFO', data['Token'], data['Symbol'], data['EXP'], data['Strike'],
                    data['OPTType'], data['Instrument'], data['Qty'], data['Price'], data['ClientID'],
                    0, data['BuySell'],
                    data['TradeNo'])

                # print('kldjk')

                self.ffff.write(trd_string)

            self.ffff.close()
            print('done')


            # trd_string = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
            # data['Client_code'], data['Exch'], data['Token'][0], data['Symbol'], data['Exp'], data['Strike'],
            # data['Opt_Type'], data['Instrument'], data['Fill_Qty'], data['Fill_Price'], data['Acc_No'],
            # data['Opt_Orderno'], data['Buy_Sell'],
            # data['tradeNO'])
            #
            # # print(today,trd_string)
            # fname = r"D:\Shared\OnlineTrades\\%s\\%s.txt" % (today, today)

            # fname = r"\\192.168.102.169\shared\OnlineTrades\\%s\\%s.txt" % (today, today)



            # ff1.to_csv('d:/ff1.csv')
            # df.to_csv('d:/Notis.csv')
            # ff.to_csv('d:/aaa.csv')
            # print(ff)

            # if notisdf.shape[0]!=0:
            #
            #     self.notisSNo+=notisdf.shape[0]
            #     # print(df.columns)
            #     # print(df.dtypes)
            #     df = notisdf.iloc[:, [2, 4, 5, 6, 7,12, 20,23, 25, 26, 27, 28]]
            #
            #     df.columns = ['TradeNo', 'Token', 'Qty', 'Price', 'BuySell', 'ClientID', 'TerminalID', 'Symbol', 'Instrument',
            #                      'EXP', 'Strike','OPTType']
            #
            #     df.drop_duplicates(subset=['TradeNo','ClientID'])
            #     # print(df.dtypes)
            #
            #     # df['TerminalID'] = df['TerminalID'].astype(str)




        except pd.errors.EmptyDataError:
            print('emptyFile')
            # print(traceback.print_exc())
        except:
            print(traceback.print_exc())
    def updateexp(self,exp):
        # exp1 = datetime.datetime.strptime(exp, '%d %b %Y').strftime('%Y%m%d')
        exp = datetime.datetime.fromtimestamp(exp)
        exp = exp.replace(2023)
        exp = datetime.datetime.strftime(exp, '%Y%m%d')
        exp=int(exp)


        return exp

    def tradeFiletoNumpy(self):
        st=time.time()

        # DCarray=pd.read_csv(self.DCpath,index_col=False,delimiter=',')
        # # DCarray=np.loadtxt(self.DCpath,dtype='str')
        # print(DCarray)

        # i=''


        # path = r'\\192.168.102.102\TradeAPI\TradeFO_23122022.txt'
        f = open(self.DCpath, 'r')
        lines1 = f.readlines()[self.rcDC:]
        #
        f=open(self.notispath,'r')
        lines = f.readlines()[self.rcnotis:]

        print(len(lines1))
        self.rcDC+=len(lines1)


        # print(lines)
        if (lines != []):
            # print(lines)
            # newLines = lines[self.rc:]

            for k in lines:
                data = k.split(',')
                tradeNo = data[2]

                dd=0
                for jk in lines1:
                    data1 = jk.split(',')
                    DCtradeNo = data1[13].split('\n')[0]
                    # print(DCtradeNo)

                    if(tradeNo==DCtradeNo):
                        dd=1
                        break

                if(dd==0):
                    print('yyyyy')
                    Clicode = data[12]
                    ctcl = data[20]
                    neatid = data[10]
                    tradeNo = data[2]

                    token = int(data[4])

                    sym = data[23]
                    exp = datetime.datetime.fromtimestamp(int(data[26]))
                    exp = exp.replace(2022)
                    exp = datetime.datetime.strftime(exp, '%Y%m%d')
                    strike = float(int(data[27]) / 100)
                    opt = data[28]
                    ins = data[25]

                    buysell = data[7]

                    TQty = int(data[5])
                    Tamt = float(int(data[6]) / 100)

                    Act_Time = datetime.datetime.fromtimestamp(int(data[17]))
                    Act_Time = Act_Time.replace(2022)
                    Act_Time = datetime.datetime.strftime(Act_Time, '%d-%m-%Y  %H:%M:%S')
                    # data = {'Exch': 'NSEFO', 'Client_code': data[10], 'Acc_No': Clicode, 'Buy_Sell': buysell,
                    #                'Fill_Qty': TQty, 'Fill_Price':
                    #                    Tamt, 'Act_Time': Act_Time, 'Opt_Orderno': 1111, 'Token': token,
                    #                'Symbol': sym,
                    #                'Exp': exp,
                    #                'Strike': strike, 'Opt_Type': opt, 'Instrument': ins, 'tradeNO': tradeNo}
                    # print(output_dict)
                    trd_string = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
                                    data[10], 'NSEFO', token, sym, exp, strike,
                                    opt, ins, TQty, Tamt, Clicode,
                                    1111, buysell,tradeNo)

                    self.ffff = open(self.DCpath, 'a+')
                    self.ffff.write(trd_string)
                    self.ffff.close()

                # print(k)
                # i = k.split(',')
                # # print(len(i))
                # Clicode = i[12]
                # ctcl = i[20]
                # neatid = i[10]
                #
                # # print('ppp',Clicode,neatid,ctcl)
                # if(Clicode == 'A1946' and neatid == '35788' and ctcl == '111111111111122' ):
                #
                #     # print('ddd',k)
                #     # print(type(k))
                #     k=k.replace("111111111111122","A1946")
                #     # print('ddf',k)
                # # token = int(i[2])
                # # dQty = int(i[8])
                # # damt = float(i[9])
                # # sym = i[3].replace(' ', '')
                # # exp = i[4]
                # #
                # # strike = format(float(i[5]), '.2f')
                # # opt = i[6]
                # # ins=i[7]
                #
                # # abc = dt.Frame([i[11],
                # #                 ['NSEFO'], [i[2]], [i[14]], [i[15]], [0], [0], [0], [0], [0], [0], [0] ]).to_numpy()
                # #
                # # abc = np.asarray([[Clicode],
                # #                 ['NSEFO'], [token],[ins], [sym], [exp], [strike], [opt], [dQty], [damt], [0], [0],[0], [0],[0],[dQty],[damt]])
                # # self.alltradearr[self.rc,:]=[Clicode,
                # #                              'NSEFO', token,ins, sym, exp,
                # #                              strike, opt, dQty, damt, 0,
                # #                              0,0, 0,0,dQty,damt]
                #
                #
                # # i=i +k
                #
                # # self.alltradearr[self.rc, :]=i
                self.rcnotis += 1








        # print(self.alltradearr)
        print('rows',self.rcnotis)
        et = time.time()
        print('time',et-st)
        # self.arr = self.alltradearr[:self.rc,:].tolist()
        # print(sys.getsizeof(self.arr))



    # def dealCommunication(self):
    #     # Get a QTcpSocket from the QTcpServer
    #
    #     self.clientConnection = self.tcpServer.nextPendingConnection()
    #
    #     while True:
    #         try:
    #             self.clientConnection.waitForReadyRead()
    #             data = self.clientConnection.read(1024)
    #
    #             if not data:
    #                 break
    #
    #             # d=data.decode()
    #             # data=json.loads(d)
    #             data=pickle.loads(data)
    #             print(data)
    #
    #             if(data['Type']=='Auth'):
    #                 if(data['User']=='Arham'):
    #                     self.status='Connected'
    #                     print('connected')
    #                 else:
    #                     self.status='Invalid User'
    #
    #                 dict = {'Type': 'AuthRes', 'status':self.status }
    #                 # jd = json.dumps(dict)
    #                 jd = pickle.dumps(dict)
    #                 self.clientConnection.write(jd)
    #
    #                 if(self.status=='Invalid User'):
    #
    #                     self.clientConnection.disconnectFromHost()
    #                 else:
    #                     self.clientList.append(self.clientConnection)
    #
    #
    #
    #             elif(data['Type']=='sendTrade'):
    #                 self.tradeFiletoNumpy()
    #                 self.timer.start()
    #             #     try:
    #             #         print(len(self.arr))
    #             #         strt=0
    #             #         while(strt<11):
    #             #             end=strt+5
    #             #             print(strt,end)
    #             #             dict = {'Type': 'Trade', 'data': self.arr[strt:end+1]}
    #             #             print(dict)
    #             #             jd = pickle.dumps(dict)
    #             #             self.clientConnection.write(jd)
    #             #             time.sleep(1)
    #             #             print('hello')
    #             #             strt=end
    #             #     except:
    #             #         print(traceback.print_exc())
    #             #     # for i in range(3):
    #             #     #     dict = {'Type': 'Trade', 'data': self.arr[:5 + 1]}
    #             #     #     print(dict)
    #             #     #     # jd = json.dumps(dict)
    #             #     #     jd = pickle.dumps(dict)
    #             #     #     self.clientConnection.write(jd)
    #             #     #     time.sleep(1)
    #             #
    #
    #
    #
    #         except:
    #             print(traceback.print_exc())
    #
    #
    #     # now disconnect connection.
    #     print('disconnected')

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
