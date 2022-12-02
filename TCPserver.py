import numpy as np
import time
import datatable as dt
import sys
import traceback
import json
import pickle
import socket

from PyQt5.QtCore import QByteArray, QDataStream, QIODevice
from PyQt5.QtWidgets import QApplication, QDialog,QMainWindow
from PyQt5.QtNetwork import QHostAddress, QTcpServer
from PyQt5.QtWidgets import  *
from PyQt5.QtCore import *



class Server(QMainWindow):
    def __init__(self):
        super(Server).__init__()

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.server.bind(('127.0.0.1', 8000))
        self.server.listen(1)









if __name__ == '__main__':
    app = QApplication(sys.argv)
    server = Server()
    # server.sessionOpened()
    sys.exit(app.exec_())



s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(('localhost', 50000))
s.listen(1)
conn, addr = s.accept()
while 1:
    data = conn.recv(1024)
    if not data:
        break
    conn.sendall(data)
conn.close()