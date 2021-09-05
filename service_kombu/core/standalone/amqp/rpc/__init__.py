#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import time
import typing as t

from kombu import Queue
from kombu import Consumer
from kombu import Exchange
from threading import Thread
from logging import getLogger
from kombu.message import Message
from kombu.exceptions import ChannelError
from kombu.exceptions import ConnectionError
from kombu.exceptions import OperationalError
from kombu.exceptions import InconsistencyError
from service_kombu.core.connect import Connection
from service_kombu.constants import KOMBU_CONFIG_KEY
from service_core.core.decorator import AsFriendlyFunc
from service_core.core.as_helper import gen_curr_request_id
from service_kombu.constants import DEFAULT_KOMBU_AMQP_REPLY_EXCHANGE_NAME

from .requests import AMQPRpcRequest
from .safekill import safe_kill_thread

logger = getLogger(__name__)


class AMQPRpcStandaloneProxy(object):
    """ AMQP RPC请求者代理 """

    def __init__(self, config: t.Dict[t.Text, t.Any], storage_buffer: t.Optional[int] = None) -> None:
        """ 初始化实例

        @param config: 配置字典
        @param storage_buffer: 缓存大小
        """
        self.config = config
        self.stopped = False
        self.queue_declared = False
        self.storage = {'_': []}
        self.correlation_id = gen_curr_request_id()
        self.storage_buffer = storage_buffer or 100
        curr_config = config.get(KOMBU_CONFIG_KEY, {})
        self.connect_options = curr_config.get('connect_options', {})
        self.consume_options = curr_config.get('consume_options', {})
        self.consume_options.update({'no_ack': True})
        self.consume_options.update({'callbacks': [self.handle_request]})
        self.consume_options.update({'queues': [self.get_queue()]})
        self.publish_options = curr_config.get('publish_options', {})
        self.consume_connect = Connection(**self.connect_options)
        self.publish_connect = Connection(**self.connect_options)
        self.publish_options.setdefault('serializer', 'json')
        self.publish_connect = Connection(**self.connect_options)
        # 应该在内部逻辑层面防止此线程异常退出但同时需要设置daemon=True和主进程一起生死与共
        name = f'amqp.rpc.standalone.proxy.consume.{self.correlation_id}'
        self.consume_thread = Thread(target=self.consume, name=name, daemon=True)

    @staticmethod
    def get_exchange() -> Exchange:
        """ 消费者使用交换机 """
        exchange_name = DEFAULT_KOMBU_AMQP_REPLY_EXCHANGE_NAME
        return Exchange(name=exchange_name, type='direct', auto_delete=True)

    def get_routing_key(self) -> t.Text:
        """ 消费者绑定路由键 """
        exchange_name = DEFAULT_KOMBU_AMQP_REPLY_EXCHANGE_NAME
        return f'{exchange_name}.amqp.rpc.standalone.proxy.{self.correlation_id}'

    def get_queue(self) -> Queue:
        """ 消费者使用的队列 """

        def on_queue_declared(*args: t.Any, **kwargs: t.Any) -> None:
            """ 回调时设置标志位

            @param args  : 位置参数
            @param kwargs: 命名参数
            @return: None
            """
            self.queue_declared = True

        exchange_name = DEFAULT_KOMBU_AMQP_REPLY_EXCHANGE_NAME
        exchange, routing_key = self.get_exchange(), self.get_routing_key()
        queue_name = f'{exchange_name}.amqp.rpc.standalone.proxy.{self.correlation_id}'
        return Queue(
            name=queue_name, exchange=exchange, routing_key=routing_key,
            durable=False, auto_delete=True, on_declared=on_queue_declared
        )

    @staticmethod
    def get_target_exchange(name: t.Text) -> Exchange:
        """ 目标的使用交换机 """
        return Exchange(name=name, type='direct', auto_delete=True)

    def _clean_storage(self) -> None:
        """ 清理当前缓存 """
        correlation_id = self.storage.get('_').pop(0)
        self.storage.pop(correlation_id, None)

    def handle_request(self, body: t.Any, message: Message) -> None:
        """ 处理工作请求 """
        correlation_id = message.properties.get('correlation_id', None)
        # 在缓存中记录所有消息的关联ID: correlation_id
        correlation_id and self.storage.get('_').append(correlation_id)
        correlation_id and self.storage.update({correlation_id: (body, message)})
        # 防止发送RPC请求但又不需要结果的情况导致内存溢出
        len(self.storage.get('_')) > self.storage_buffer and self._clean_storage()

    def consume(self) -> None:
        """ 自动消费消息 """
        consumer, consume_connect_loss = None, False
        while not self.stopped:
            try:
                if consume_connect_loss is True:
                    logger.debug(f'{self} consume_connect loss, start reconnecting')
                    self.consume_connect = Connection(**self.connect_options)
                    consume_connect_loss = False
                consumer = Consumer(self.consume_connect, **self.consume_options)
                consumer.consume()
                logger.debug(f'{self} start consuming with {self.consume_options}')
                while not self.stopped: self.consume_connect.drain_events()
                # 优雅处理如ctrl + c, sys.exit, kill thread时的异常
            except (KeyboardInterrupt, SystemExit):
                break
                # 优雅处理ConnectionError等连接异常断开异常会去自动重试
            except (ConnectionError, ChannelError, OperationalError, InconsistencyError):
                consume_connect_loss = True
                # 如果之前建立过连接,暂不关心当前连接状态强制关闭掉当前连接
                self.consume_connect and AsFriendlyFunc(self.consume_connect.release)()
                logger.error(f'consume_connect error while consumer consume', exc_info=True)
                time.sleep(2)
            except:
                # 应该避免其它未知异常中断当前消费者导致任务无法被及时消费
                logger.error(f'unexpected error while consumer consume', exc_info=True)
                time.sleep(1)

    def __enter__(self):
        """ 创建时回调 """
        self.consume_thread.start()
        # 发送消息前必须保证回复队列已经成功声明否则收不到消息
        while not self.queue_declared: time.sleep(0.01)
        return AMQPRpcRequest(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ 销毁时回调 """
        self.stopped = True
        # 使用ctypes模块根据consume线程标识向其发送终止信号
        safe_kill_thread(self.consume_thread, SystemExit)
        # TODO: 调用consume和publish的release会阻塞无返回
        # 由于默认已经开启心跳机制所以此处服务端会自行回收连接
        del self.consume_connect, self.publish_connect
