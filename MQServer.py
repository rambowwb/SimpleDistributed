#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import RabbitMQModel
import logging
import os
import hashlib
import pathlib
import datetime
import json
import base64

RabbitMQURL = 'amqp://guest:guest@localhost:5672/%2F'

def SetLogging(Level, Path, FileName, Format = '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
    if pathlib.Path(Path).is_dir() == False:
        os.makedirs(Path)
    logging.basicConfig(level = Level,
                        filename = Path + "/" + FileName + "_" + str(os.getpid()) + "_" + datetime.datetime.now().strftime('%H:%M:%S') + ".log",
                        filemode = 'w',
                        format = Format
)

def SendServerModel():
    global RabbitMQURL
    SendMsg = {}
    with open('./ServerModel.py', 'r') as F:
        SendMsg['Code'] = base64.b64encode(F.read().encode()).decode()
        SendMsg['CodeMD5'] = hashlib.md5(SendMsg['Code'].encode()).hexdigest()
    return RabbitMQModel.SendMsg(RabbitMQURL,
                                'RabbitMQSwitch',
                                json.dumps(SendMsg),
                                { 'Broadcast' : True }
    )    

def ServerMessCallBack(Channel, BasicDeliver, Properties, Body, Par, IsBroadcast):
    Body = json.loads(Body.decode())
    if Body['LogType'] == "Warning":
        logging.warning(str(Body))
    elif Body['LogType'] == "Info":        
        logging.info(str(Body))
        if Body['Type'] == "Online":
            Res = SendServerModel()
            if Res[0] == False:
                logging.warning(Res[1])
    return True

def main():
    global RabbitMQURL
    SetLogging(logging.INFO, "./ServerLog", "Server")
    RabbitMQModel.PikaConsumer(RabbitMQURL,
                               'RabbitMQSwitch',
                               'ServerListen',
                               ServerMessCallBack,
                               { 'Sever' : True }
    )

if __name__ == '__main__':
    main()
