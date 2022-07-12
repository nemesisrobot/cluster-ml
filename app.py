from flask import Flask, redirect, request
import socket
import random

'''
Configuração de acesso ao cluster de processamento
'''
HOSTCLUSTER='127.0.0.1'
PORTCLUSTER=5001
PORTCLUSTER2=5002

con =socket.socket(socket.AF_INET,socket.SOCK_STREAM)
con.connect((HOSTCLUSTER,PORTCLUSTER))

con2 = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
con2.connect((HOSTCLUSTER,PORTCLUSTER2))


#nocluster=0

def conexaoCluster2(dados,con2):
   print(type(((dados['nome']).encode())))
   paylod = '{}\r\n'.format(dados['nome']).encode(encoding = 'UTF-8')
   print(paylod)
   con2.send(paylod)

def conexaoCluster(dados,con):
    print(type(((dados['nome']).encode())))
    #con.send(b'\r\n')
    paylod = '{}\r\n'.format(dados['nome']).encode(encoding = 'UTF-8')
    print(paylod)
    print(type(paylod))
    con.send(paylod)
    #con.sendall()
    #con.close()

app = Flask(__name__)

@app.route('/')
def index():
    return 'Dados entregues ao Cluster'

@app.route('/save', methods=['POST'])
def save():

    dados = request.get_json()
    print(dados)
    if(random.randint(0, 1)==0):
        conexaoCluster(dados,con)
        #nocluster=nocluster+1
    else:
       conexaoCluster2(dados,con2)
       #nocluster=nocluster-1
    return redirect('/')

app.run(host='0.0.0.0')
