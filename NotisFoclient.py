import json
import traceback
import pickle

from PyQt5.QtCore import *

from PyQt5.QtCore import QDataStream, QIODevice
from PyQt5.QtWidgets import *
from PyQt5.QtNetwork import QTcpSocket, QAbstractSocket

class Client(QMainWindow):
    def __init__(self):
        super().__init__()
        self.tcpSocket = QTcpSocket(self)

        self.makeRequest()
        self.isLoggedIn = False
        self.tcpSocket.waitForConnected(1000)
        # send any message you like it could come from a widget text.
        # self.tcpSocket.write(b'hello')
        self.tcpSocket.connected.connect(self.on_connect)
        # self.tcpSocket.readyRead.connect(self.On_readyRead)
        self.tcpSocket.error.connect(self.displayError)
        self.tcpSocket.disconnected.connect(self.on_disconeect)

        self.timer=QTimer()
        self.timer.setInterval(1)
        self.timer.timeout.connect(self.On_readyRead)
        self.timer.start()

        self.le = QLineEdit(self)
        self.le.setGeometry(20, 20, 200, 20)

        self.pb = QPushButton(self)
        self.pb.setGeometry(20, 70, 200, 20)
        self.pb.clicked.connect(self.sendD)
        self.c=0


    def on_connect(self):
        print('connected')

    def on_disconeect(self):
        print('disconnected')
        self.makeRequest()

    def makeRequest(self):
            HOST = '127.0.0.3'
            PORT = 30698
            self.tcpSocket.connectToHost(HOST, PORT, QIODevice.ReadWrite)

    def sendD(self):
        try:
            msg = self.le.text()
            dict={'Type':'Auth','User':msg}
            # jd=json.dumps(dict)
            jd=pickle.dumps(dict)
            # print(msg)
            self.tcpSocket.write(jd)
        except:
            print(traceback.print_exc())


    def sendtrade(self):
        dict = {'Type': 'sendTrade', 'Start':0 }
        # jd = json.dumps(dict)
        jd=pickle.dumps(dict)
        # print(msg)
        self.tcpSocket.write(jd)

    def On_readyRead(self):
        try:


            print('hello')


            if(self.isLoggedIn==False):

                # d = data.decode()
                data = self.tcpSocket.read(1024)
                data = pickle.loads(data)
                if(len(data)>1):


                    if (data['Type'] == 'AuthRes'):
                        if(data['status']=='Connected'):
                            print('Connected...')
                            self.sendtrade()
                            self.isLoggedIn=True


                        else:
                            print('Retry')

            else:

                data = self.tcpSocket.read(220)
                print(data)


                if(len(data)>1):
                    self.c+=1
                    print(self.c)


        # elif (data['Type'] == 'Trade'):
            #     print('inelse')
            #     d=data['data']
            #     print(d)
            #






        except:
            print(traceback.print_exc())







    def displayError(self, socketError):
        if socketError == QAbstractSocket.RemoteHostClosedError:
            pass
        else:
            print(self, "The following error occurred: %s." % self.tcpSocket.errorString())


if __name__ == '__main__':
    import sys

    app = QApplication(sys.argv)
    client = Client()
    client.show()
    sys.exit(app.exec_())


