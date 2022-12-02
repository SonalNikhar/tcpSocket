import socket
import sys
from PyQt5.QtCore import QByteArray, QDataStream, QIODevice
from PyQt5.QtWidgets import QApplication, QDialog,QMainWindow
from PyQt5.QtNetwork import QHostAddress, QTcpServer
# from PyQt5.QtWidgets import  *
from PyQt5.QtCore import *



class Client(QMainWindow):
    def __init__(self):
        super(Client).__init__()

        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect(('127.0.0.1', 50000))








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