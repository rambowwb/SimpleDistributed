#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import RabbitMQModel
import hashlib
import json
import base64
import uuid

RabbitMQURL = 'amqp://admin:admin@localhost:5672/%2F'

def RPCFunc(CodesAndPar):
    global RabbitMQURL
    SendMsg = {}
    with open('./ServerModel.py', 'r') as F:
        R = base64.b64encode(F.read().encode()).decode()
        SendMsg['CodeMD5'] = hashlib.md5(R.encode()).hexdigest()
    SendMsg['Codes'] = CodesAndPar
    SendMsg['UUID'] = str(uuid.uuid4())

    Res = RabbitMQModel.SendMsg(RabbitMQURL,
                                'RabbitMQSwitch',
                                json.dumps(SendMsg),
                                { 'Client' : True }
    )

    if Res[0] == False:
        return Res[1]
    else:
        return None

def main():
    Res = RPCFunc([('TestDistributed', None)])
    if Res is not None:
        print(Value + " : " + Res)
    
if __name__ == '__main__':
    main()

