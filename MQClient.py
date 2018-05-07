#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import RabbitMQModel
import logging
import os
import hashlib
import sys
import importlib
import socket
import datetime
import pathlib
import json
import base64
import time
    
PID = str(os.getpid())
CodeMD5 = None
ImportOB = None
IPAddr = socket.gethostbyname(socket.gethostname())
RabbitMQURL = "amqp://admin:admin@localhost:5672/%2F"


def SetLogging(Level, Path, FileName, Format = '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
    if pathlib.Path(Path).is_dir() == False:
        os.makedirs(Path)
    logging.basicConfig(level = Level,
                        filename = Path + "/" + FileName + "_" + str(os.getpid()) + "_" + datetime.datetime.now().strftime('%H:%M:%S') + ".log",
                        filemode = 'w',
                        format = Format
)

def SendServer(LogType, Type, M):
    global PID
    global IPAddr
    global RabbitMQURL
    Msg = {}
    Msg['LogType'] = LogType
    Msg['Type'] = Type
    Msg['IP'] = IPAddr
    Msg['PID'] = PID
    Msg['Msg'] = M
    Msg["Time"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    Res = RabbitMQModel.SendMsg(RabbitMQURL,
                                'RabbitMQSwitch',
                                json.dumps(Msg),
                                { 'Sever' : True })
    if Res[0] == False:
        logging.warning(Res[1])
        return Res[1]
    else:
        return None
        
def ImportServerCode(Code, MD5, Path = ".", ImportName = "ImportClient"):
    global ImportOB
    global CodeMD5

    if pathlib.Path(Path).is_dir() == False:
        os.makedirs(Path)
    sys.path.append(Path)
    with open(Path + '/' + ImportName + '.py', 'w') as F:
        F.write(base64.b64decode(Code).decode())
    try:
        ImportOB = importlib.import_module(ImportName) if ImportOB is None else importlib.reload(ImportOB)
        CodeMD5 = MD5
    except Exception as Err:
        CodeMD5 = None
        logging.warning(str(Err))
        SendServer('Warning', 'ImportClient', CodeMD5)        

def RunServerCode(Codes):
    global ImportOB
    Res = None
    for Code in Codes:
        try:
            Res = getattr(ImportOB, Code[0])(Res, Code[1])
        except Exception as Err:
            logging.warning(str(Err))
            SendServer('Warning', 'RunClient', str(Err))
            return
    SendServer('Info', 'RunClient', "Done : " + ("" if Res == None else str(Res)))

def ClearTimeOutPar(Dict, KeyName, Time, Threshold):
    list(map(lambda K: Dict[KeyName].pop(K), [Key for Key, Value in Dict[KeyName].items() if (Time - Value) > Threshold]))
            
def ClientMessCallBack(Channel, BasicDeliver, Properties, Body, Par, IsBroadcast):
    global CodeMD5
    global PID
    global IPAddr
    CurTime = int(time.time())

    ClearTimeOutPar(Par, 'Par', CurTime, 60)
    
    Body = json.loads(Body.decode())
                  
    if IsBroadcast == True:
        if Body['CodeMD5'] != CodeMD5:
            ImportServerCode(Body['Code'], Body['CodeMD5'], Path = "./ImportClient", ImportName = PID)
    else:
        if Body['CodeMD5'] != CodeMD5:
            if Body['UUID'] in Par['Par']:
                if (CurTime - Par['Par'][Body['UUID']]) > 8:
                    Par['Par'].pop(Body['UUID'])
                    return True
                else:
                    return False
            logging.warning("ClientMessCallBack CodeMd5 Err! : " + ("None" if CodeMD5 is None else CodeMD5))
            Par['Par'][Body['UUID']] = CurTime
            SendServer("Info", "Online", "")
            return False
        else:
            RunServerCode(Body['Codes'])
            if Body['UUID'] in Par['Par']:
                Par['Par'].pop(Body['UUID'])
            
    return True
    
def main():
    global RabbitMQURL
    SetLogging(logging.INFO, "./ClientLog", "Client")
    RabbitMQModel.PikaConsumer(RabbitMQURL,
                               'RabbitMQSwitch',
                               'ClientListen',
                               ClientMessCallBack,
                               { 'Client' : True },
                               BroadcastArg = { 'Broadcast' : True }
    )

if __name__ == '__main__':
    main()



