import socket
import time
from threading import Thread

class ProcessoCluster(Thread):

    def __init__(self, dado):
        Thread.__init__(self)
        self._dado= dado

    def run(self):
        print('Processando {}'.format(self._dado))
        time.sleep(10)
        print('Dado {} Processado'.format(self._dado))

HOST=''
PORT=5002

tcp = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
tcp.bind((HOST,PORT))
tcp.listen(10)

print('---------Iniciando Nó de Processamento--------------')
while True:
    con, client =tcp.accept()
    print('Recebendo dados!!!')
    while True:
        msg = con.recv(1024)
        if not msg: break
        processo = ProcessoCluster(msg)
        processo.start()
        con.send(b'Fim do processo\r\n')

    print('Finalizando conexão')
    con.close()




