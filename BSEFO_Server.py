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


class Server(QMainWindow):
    def __init__(self):
        super().__init__()
        self.rc = 0
        self.len=0
        self.fc = 0


        self.tcpServer = QTcpServer(self)
        PORT = 22888
        address = QHostAddress('192.168.130.42')



        self.today = datetime.datetime.today().strftime("%d")
        print(self.today)
        self.path = r'\\192.168.102.111\Export\6405AD2TR' + self.today + '01.csv'

        self.notisSNo=1
        self.maindf=pd.DataFrame()

        self.clientList = []

        self.Expense()
        self.getBSEcontraact()

        self.tcpServer.newConnection.connect(self.dealCommunication)

        self.timer=QTimer()
        self.timer.setInterval(10000)
        self.timer.timeout.connect(self.On_readyRead1)
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


    def getBSEcontraact(self):

        mongoclient = MongoClient("192.168.130.40", 27017)
        BOD_Data = mongoclient['BOD']
        BSEcontractfile=BOD_Data['BSE_contractFO']

        self.dbBOD = list(BSEcontractfile.find({},{'_id':0,'Token':1,'InstrumentType':1,'Exp':1,'Strike':1,'OptionType':1,'Symbol':1}))
        self.contract = pd.DataFrame(self.dbBOD)
        # [
        #     ['Token', 'AssetToken', 'InstrumentType', 'Symbol', 'UnderlyingAsset', 'exp', 'strike', 'OptionType', 'Precision','PartitionID',
        #      'contractStartDate','settlementDate','ProductID','LotSize','LotQty','TickSize','InstumentID','ContractEXcDate','ContractEndDate',
        #      'QtyMultiplier','SeriesCode','InstrumentName','UnderlyingMarket','ContractType','ProductCode','BasePrice','DeleteFlag']].values
        # self.contract.to_csv('contract.csv')






    def On_readyRead1(self):
        try:
            st = time.time()
            print('ssss')

            # df1 = pd.read_csv(f'{self.FilePath}{self.today}.txt', header=None, dtype={20: str},skiprows=self.notisSNo)

            # print('notis',self.notisSNo)
            # print(df1)

            df1=pd.read_csv(self.path,header=None,dtype={21:str,2:int,4:int},skiprows=self.notisSNo)

            self.notisSNo = self.notisSNo+df1.shape[0]
            # print(df1)
            df1 = df1[df1[22]!=1]
            # print(df1[22])

            if df1.shape[0] != 0:


                # df1.to_csv('df1.csv')
                # print(self.contract.dtypes)


                df1 = pd.merge(self.contract, df1, how='right', left_on=['Token'],
                               right_on=[2])

                df1.to_csv('dfupdate.csv')




                no=self.maindf.shape[0]
                self.maindf = pd.concat([self.maindf, df1],
                                        axis=0,ignore_index=True)
                # no=self.notisSNo




                df1 = self.maindf

                if df1.shape[0] != 0:

                    # print(df.columns)
                    # print(df.dtypes)
                    df1 = df1.iloc[:, [0,11,10,19,21,27,2,1,3,4,5]]


                    df1.columns = [ 'Token', 'Qty', 'Price', 'BuySell', 'ClientID', 'TerminalID', 'Symbol',
                                   'Instrument',
                                   'EXP', 'Strike', 'OPTType']
                    # print('dfj',df.shape)
                    df = df1.drop_duplicates()
                    df = df.loc[no:, :]







                    # df2 = df
                    # print(df.shape)
                    self.rc += df.shape[0]
                    print('rc', self.rc)

                    convert_dict = {'TerminalID': str, 'ClientID': str }

                    df = df.astype(convert_dict)
                    # print(df)

                    # print(df.dtypes)

                    df['TerminalID'] = df['TerminalID'].astype(str)

                    df["Qty"] = np.where(df['BuySell'] == 1, df['Qty'], -df['Qty'])
                    df["TerminalID"] = np.where(df['TerminalID'] == '0', df['ClientID'], df['TerminalID'])
                    df["Price"] = df["Price"] / 100
                    # df["Strike"] = df["Strike"] / 100
                    # df["EXP"] = df["EXP"].apply(self.updateexp)
                    # float(int(data[6]) / 100) * -TQty

                    df["Tradeamt"] = -df["Qty"] * df["Price"]
                    df["Val"] = abs(df["Tradeamt"]) / 10000000



                    df["TOC"] = np.where(df['OPTType'] == ' ', np.where(df['BuySell'] == 1, (df['Val'] * self.f_buyex),
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

                    df.to_csv('dddgruopby.csv')
                    print('jjj')


                    # for i in df.to_numpy():
                    #     # print(type(row))
                    # self.sgdatasend.emit(i)

                    # df.to_csv('d:/aaa.csv')
                    # print('fjdf')
                    for row in df.values.tolist():
                        # print('kfjdkfj')

                        for izzz in self.clientList:
                            row = str(row)
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
            et = time.time()
            print('et', et - st)

        except pd.errors.EmptyDataError:
            print('dfdf')
        except:
            print(traceback.print_exc())




    def updateexp(self,exp):
        # exp1 = datetime.datetime.strptime(exp, '%d %b %Y').strftime('%Y%m%d')
        exp = datetime.datetime.fromtimestamp(exp)
        exp = exp.replace(2023)
        exp = datetime.datetime.strftime(exp, '%Y%m%d')

        return exp






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






















