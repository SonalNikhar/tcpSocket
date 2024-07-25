
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
        self.DCpath = r'\\192.168.102.112\Shared\OnlineTrades\\%s\\%sCM.txt' %(self.today1,self.today1)

        self.notispath = r'\\192.168.102.102\TechExcel\TradeAPI\TradeCM_' + self.today + '.txt'

        self.timer=QTimer()
        self.timer.setInterval(2000)
        self.timer.timeout.connect(self.On_readyRead)
        self.timer.start()




    def On_readyRead(self):
        try:
            st=time.time()
            DCdf = pd.read_csv(self.DCpath, header=None, skiprows=self.DCSno)
            self.DCSno += DCdf.shape[0]

            notisdf=pd.read_csv(self.notispath,header=None,skiprows=self.notisSNo,dtype={20:str})
            self.notisSNo += notisdf.shape[0]

            # df = notisdf.iloc[:, [2,12]]
            notisdf = notisdf.iloc[:, [0, 5, 6, 7, 12, 26, 27]]

            notisdf.columns = ['TradeNo', 'Qty', 'Price', 'BuySell', 'ClientID', 'Symbol', 'Series',
                          ]

            convert_dict = {'ClientID': str
                            }

            notisdf = notisdf.astype(convert_dict)


            notisdf.drop_duplicates(subset=['TradeNo', 'ClientID'])


            notisdf["Price"] = notisdf["Price"] / 100




            DCdf = DCdf.iloc[:, [9,4,5,7,2,3,8]]

            DCdf.columns = ['TradeNo', 'Qty', 'Price', 'BuySell', 'ClientID', 'Symbol', 'Series',
                          ]

            convert_dict = {'ClientID': str
                            }

            DCdf = DCdf.astype(convert_dict)




            i1 = pd.MultiIndex.from_frame(notisdf)
            i2 = pd.MultiIndex.from_frame(DCdf)

            dd=notisdf[~i1.isin(i2)]

            self.ffff = open(self.DCpath, 'a+')
            #
            for index, data in dd.iterrows():
                # print(row["Name"], row["Age"])
                trd_string = '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (0,'NSECM',data['ClientID'],data['Symbol'],data['Qty'],data['Price'],
                                                                  0,data['BuySell'],data['Series'],data['TradeNo'])

                self.ffff.write(trd_string)

            self.ffff.close()






        except pd.errors.EmptyDataError:
            print('dfdf')
        except:
            print(traceback.print_exc())
    def updateexp(self,exp):
        # exp1 = datetime.datetime.strptime(exp, '%d %b %Y').strftime('%Y%m%d')
        exp = datetime.datetime.fromtimestamp(exp)
        exp = exp.replace(2023)
        exp = datetime.datetime.strftime(exp, '%Y%m%d')
        exp=int(exp)


        return exp



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
