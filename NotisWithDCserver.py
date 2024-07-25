
import pandas as pd
import datetime

import time

import sys
import traceback

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


        self.today = datetime.datetime.today().strftime('%d%m%Y')

        self.today1 = datetime.datetime.today().strftime('%Y%m%d')
        self.DCpath = r'\\192.168.102.112\Shared\OnlineTrades\%s\%s.txt' % (self.today1, self.today1)
        # self.DCpath = r'\\192.168.102.112\Shared\OnlineTrades\20230428\20230428.txt'
        self.notispath = r'\\192.168.102.102\TechExcel\TradeAPI\TradeFO_' + self.today + '.txt'

        self.timer=QTimer()
        self.timer.setInterval(2000)
        self.timer.timeout.connect(self.On_readyRead)
        self.timer.start()




    def On_readyRead(self):
        try:
            st=time.time()
            DCdf = pd.read_csv(self.DCpath, header=None)
            self.DCSno += DCdf.shape[0]

            notisdf=pd.read_csv(self.notispath,header=None,dtype={20:str})
            self.notisSNo += notisdf.shape[0]


            notisdf = notisdf.iloc[:, [2, 4, 5, 6, 7, 12, 23, 25, 26, 27, 28]]

            notisdf.columns = ['TradeNo', 'Token', 'Qty', 'Price', 'BuySell', 'ClientID', 'Symbol',
                          'Instrument','EXP', 'Strike', 'OPTType']

            convert_dict = { 'ClientID': str
                            }

            notisdf = notisdf.astype(convert_dict)


            notisdf=notisdf.drop_duplicates()


            notisdf["Price"] = notisdf["Price"] / 100
            notisdf["Strike"] = notisdf["Strike"] / 100
            notisdf["EXP"] = notisdf["EXP"].apply(self.updateexp)




            DCdf = DCdf.iloc[:, [13,2,8,9,12,10,3,7,4,5,6]]

            DCdf.columns = ['TradeNo', 'Token', 'Qty', 'Price', 'BuySell', 'ClientID', 'Symbol',
            'Instrument',
            'EXP', 'Strike', 'OPTType']

            convert_dict = {'ClientID': str
                            }

            DCdf = DCdf.astype(convert_dict)

            i1 = pd.MultiIndex.from_frame(notisdf)
            i2 = pd.MultiIndex.from_frame(DCdf)

            dd=notisdf[~i1.isin(i2)]
            dd.to_csv('DCNew.csv')


            self.ffff = open(self.DCpath, 'a+')

            for index, data in dd.iterrows():

                trd_string = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
                    0, 'NSEFO', data['Token'], data['Symbol'], data['EXP'], data['Strike'],
                    data['OPTType'], data['Instrument'], data['Qty'], data['Price'], data['ClientID'],
                    0, data['BuySell'],
                    data['TradeNo'])



                self.ffff.write(trd_string)

            self.ffff.close()
            print('done')


        except pd.errors.EmptyDataError:
            print('emptyFile')
            # print(traceback.print_exc())
        except:
            print(traceback.print_exc())
    def updateexp(self,exp):
        # exp = datetime.datetime.fromtimestamp(exp)
        exp = exp.replace(2023)
        exp = datetime.datetime.strftime(exp, '%Y%m%d')
        exp=int(exp)


        return exp




if __name__ == '__main__':
    app = QApplication(sys.argv)
    server = Server()

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
