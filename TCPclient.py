import socket
import sys
import traceback

from PyQt5.QtCore import QByteArray, QDataStream, QIODevice
from PyQt5.QtWidgets import QApplication, QDialog,QMainWindow
from PyQt5.QtNetwork import QHostAddress, QTcpServer
# from PyQt5.QtWidgets import  *
from PyQt5.QtCore import *
import csv


class Client(QMainWindow):
    def __init__(self):
        super().__init__()
        self.rcN1 = 0
        self.rc1 = 0
        self.rcN2 = 0
        self.rc2 = 0

        self.timerE=QTimer()
        self.timerE.setInterval(10000)
        self.timerE.timeout.connect(self.readappend)
        self.timerE.start()

        self.timerEN = QTimer()
        self.timerEN.setInterval(15000)
        self.timerEN.timeout.connect(self.readappendNotis)
        self.timerEN.start()

        # print('kk')


    def readappend(self):
        # print('kj')
        try:
            sourceFile = open(r'\\192.168.102.112\Shared\OnlineTrades\\20230519\\20230519.txt', 'r')

            data = sourceFile.readlines()[self.rc1:self.rc1+10000]
            self.rc1=self.rc1+10000

            with open(r'\\192.168.102.112\Shared\OnlineTrades\\20230520\\20230520.txt', 'a') as destFile:
                for i in data:
                    destFile.write(i)

            sourceFile = open(r'\\192.168.102.112\Shared\OnlineTrades\\20230519\\20230519CM.txt', 'r')

            # print('rcN1',self.rcN1)
            data = sourceFile.readlines()[self.rcN1:self.rcN1 + 20]

            self.rcN1 = self.rcN1 + 20
            # print('rcN1A', self.rcN1)
            with open(r'\\192.168.102.112\Shared\OnlineTrades\\20230520\\20230520CM.txt', 'a') as destFile:
                for i in data:
                    destFile.write(i)

            print('done')
        except:
            print(traceback.print_exc())
    def readappendNotis(self):
        # print('kj')
        try:
            sourceFile = open(r'\\192.168.102.102\TradeAPI\TradeFO_19052023.txt', 'r')

            data = sourceFile.readlines()[self.rc2:self.rc2+10000]
            self.rc2=self.rc2+10000

            with open(r'\\192.168.102.102\TradeAPI\TradeFO_20052023.txt', 'a') as destFile:
                for i in data:
                    destFile.write(i)

            sourceFile = open(r'\\192.168.102.102\TradeAPI\TradeCM_19052023.txt', 'r')

            data = sourceFile.readlines()[self.rcN2:self.rcN2 + 20]
            self.rcN2 = self.rcN2 + 20

            with open(r'\\192.168.102.102\TradeAPI\TradeCM_20052023.txt', 'a') as destFile:
                for i in data:
                    destFile.write(i)

            print('done')
        except:
            print(traceback.print_exc())





if __name__ == '__main__':
    app = QApplication(sys.argv)
    server = Client()
    # server.sessionOpened()
    sys.exit(app.exec_())



# s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect(('127.0.0.1', 8000))
#
# s.sendall('Hello, world')
# data = s.recv(1024)
# s.close()
# print ('Received', repr(data))