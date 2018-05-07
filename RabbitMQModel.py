#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pika
from functools import wraps
import time
import collections

def LogFunName(Func):
    @wraps(Func)
    def WrapFunc(*args, **kwargs):
#        print(Func.__name__)
        return Func(*args, **kwargs)
    return WrapFunc

class PikaConsumer(object):
    @LogFunName
    def __init__(self,
                 AmqpUrl,
                 ExchangeName,
                 QueueName,
                 QueueCallBack,
                 BindArg,
                 ConnTimeOutFunc = None,
                 DeleteExchange = False,
                 DeleteQueue = False,
                 NeedDurable = False,
                 BroadcastArg = None,
                 PrefetchCount = 1):
        self._Url = AmqpUrl
        self._ExchangeName = ExchangeName
        self._QueueName = QueueName
        self._QueueCallBack = QueueCallBack
        self._BindArg = BindArg
        self._ConnTimeOutFunc = ConnTimeOutFunc
        self._Channel = None
        self._Stopping = True
        self._ConsumerTag = None
        self._CallBackPar = collections.defaultdict(dict)
        self._TimeOutPar = collections.defaultdict(dict)
        self._ConnTimeOutID = None
        self._DeleteExchange = DeleteExchange
        self._DeleteQueue = DeleteQueue
        self._NeedDurable = NeedDurable
        self._BroadcastArg = BroadcastArg
        self._PrefetchCount = PrefetchCount
        self._Run()

    @LogFunName        
    def _Connect(self):
        return pika.SelectConnection(pika.URLParameters(self._Url),
                                     on_open_callback = self._OpenChannel,
                                     on_close_callback = self._OnConnectionClosed,
                                     stop_ioloop_on_close = False)

    @LogFunName    
    def _OnConnectionClosed(self, Connection, ReplyCode, ReplyText):
        if self._Stopping == True:
            self._Connection.ioloop.stop()
            self._Connection = None
        else:
            self._Connection.add_timeout(5, self._Reconnect)
            
    @LogFunName            
    def _Reconnect(self):
        self._Connection.ioloop.stop()
        if self._Stopping == False:
            self._Run()

    @LogFunName        
    def _OpenChannel(self, Unused):
        if self._ConnTimeOutFunc is not None:
            self._ConnTimeOutID = self._Connection.add_timeout(0, self._OnConnTimeOutFunc)
        
        self._Connection.channel(on_open_callback = self._OnChannelOpen)

    @LogFunName
    def _OnConnTimeOutFunc(self):
        self._ConnTimeOutID = None
        R = self._ConnTimeOutFunc(self._TimeOutPar)
        if R[0] == True:
            self._ConnTimeOutID = self._Connection.add_timeout(R[1], self._OnConnTimeOutFunc)
        
    @LogFunName        
    def _OnChannelOpen(self, Channel):
        self._Channel = Channel
        self._Channel.basic_qos(prefetch_count = self._PrefetchCount)
        self._Channel.add_on_close_callback(self._OnChannelClosed)
        self._SetupExchange()

    @LogFunName        
    def _SetupExchange(self):
        if self._DeleteExchange == True:
            self._Channel.exchange_delete(exchange = self._ExchangeName)
        self._Channel.exchange_declare(self._SetupQueue,
                                       self._ExchangeName,
                                       'headers',
                                       durable = self._NeedDurable)
    
    @LogFunName        
    def _OnChannelClosed(self, Channel, ReplyCode, ReplyText):
        self._Channel = None
        self._Connection.close()

    @LogFunName        
    def _SetupQueue(self, Unused):
        if self._DeleteQueue == True:
            self._Channel.queue_delete(queue = self._QueueName)
        if self._BroadcastArg is not None:
            self._Channel.queue_declare(self._SetupBind, exclusive = True, auto_delete = True)
        self._Channel.queue_declare(self._SetupBind, self._QueueName, durable = self._NeedDurable)

    @LogFunName        
    def _SetupBind(self, Queue):
        if Queue.method.queue == self._QueueName:
            self._Channel.queue_bind(self._OnBindDone, Queue.method.queue, self._ExchangeName, arguments = self._BindArg)
        else:
            self._BroadcastQueueName = Queue.method.queue
            self._Channel.queue_bind(self._OnBindTmpDone, Queue.method.queue, self._ExchangeName, arguments = self._BroadcastArg)

    @LogFunName            
    def _OnBindTmpDone(self, unused_frame):
        self._Channel.basic_consume(self._OnMessageTmp, self._BroadcastQueueName)
        
    @LogFunName            
    def _OnBindDone(self, unused_frame):
        self._Channel.add_on_cancel_callback(self._OnConsumerCancelled)
        self._ConsumerTag = self._Channel.basic_consume(self._OnMessage, self._QueueName)

    @LogFunName            
    def _OnMessageTmp(self, Channel, BasicDeliver, Properties, Body):
        if self._QueueCallBack(Channel, BasicDeliver, Properties, Body, self._CallBackPar, True) == True:
            Channel.basic_ack(delivery_tag = BasicDeliver.delivery_tag)
        else:
            Channel.basic_reject(delivery_tag = BasicDeliver.delivery_tag)
    
            
    @LogFunName        
    def _OnMessage(self, Channel, BasicDeliver, Properties, Body):
        if self._QueueCallBack(Channel, BasicDeliver, Properties, Body, self._CallBackPar, False) == True:
            Channel.basic_ack(delivery_tag = BasicDeliver.delivery_tag)
        else:
            Channel.basic_reject(delivery_tag = BasicDeliver.delivery_tag)
            
    @LogFunName        
    def _OnConsumerCancelled(self, method_frame):
        self._ConsumerTag = None
        self._CloseChannel()

    @LogFunName        
    def _OnCancelOk(self, unused_frame):
        self._ConsumerTag = None
        self._CloseChannel()

    @LogFunName                
    def _CloseConnection(self):
        if self._ConnTimeOutID is not None:
            self._Connection.remove_timeout(self._ConnTimeOutID)
            self._ConnTimeOutID = None                
        if self._Connection.is_open == True:
            self._Connection.close()
        else:
            self._Connection.ioloop.stop()

    @LogFunName                    
    def _CloseChannel(self):
        self._BroadcastQueueName = None
        if self._Channel.is_open == True:
            self._Channel.close()
            
    @LogFunName        
    def _StopConsuming(self):
        if self._ConsumerTag is not None:
            self._Channel.basic_cancel(self._OnCancelOk, self._ConsumerTag)
        elif self._Channel is not None:
            self._CloseChannel()
        elif self._Connection is not None:
            self._CloseConnection()
            
    @LogFunName        
    def _Run(self):
        while True:
            self._Stopping = False
            try:
                self._Connection = self._Connect()
                self._Connection.ioloop.start()
            except KeyboardInterrupt:
                self.Stop()
                break
            except Exception as Err:
                self._Stopping = True
                time.sleep(10)

    @LogFunName        
    def Stop(self):
        self._Stopping = True
        if self._ConnTimeOutID is not None:
            self._Connection.remove_timeout(self._ConnTimeOutID)
            self._ConnTimeOutID = None
        self._StopConsuming()
        self._Connection.ioloop.start()        


def SendMsg(URL, ExchangeName, Msg, SendArg):
    try:
        Connection = pika.BlockingConnection(pika.URLParameters(URL))
        Channel = Connection.channel()
        Channel.confirm_delivery()
        Res = Channel.basic_publish(exchange = ExchangeName,
                                    routing_key = '',
                                    body = Msg,
                                    properties = pika.BasicProperties(
                                        headers = SendArg,
                                    ),
                                    mandatory = False
        )
        Channel.close()
        Connection.close()
        return (Res, None)
    except Exception as Err:
        return (False, str(Err))
        

    
