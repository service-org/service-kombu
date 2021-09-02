#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from kombu import Queue
from kombu import Exchange
from logging import getLogger
from kombu.message import Message
from service_kombu.core.connect import Connection
from service_core.core.context import WorkerContext
from service_kombu.constants import KOMBU_CONFIG_KEY
from service_core.core.as_helper import gen_curr_request_id
from service_core.core.service.dependency import Dependency
from service_kombu.constants import DEFAULT_KOMBU_AMQP_HEARTBEAT
from service_kombu.constants import DEFAULT_KOMBU_AMQP_REPLY_EXCHANGE_NAME

from .requests import AMQPRpcRequest
from .producer import AMQPRpcProxyProducer

logger = getLogger(__name__)


class AMQPRpcProxy(Dependency):
    """ AMQP RPC请求代理类 """

    name = 'AMQPRpcProxy'

    producer = AMQPRpcProxyProducer()

    def __init__(
            self,
            alias: t.Text,
            connect_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            consume_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            publish_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            **kwargs: t.Text
    ) -> None:
        """ 初始化实例

        @param alias: 配置别名
        @param connect_options: 连接配置
        @param consume_options: 消费配置
        @param publish_options: 发布配置
        @param kwargs: 其它配置
        """
        self.alias = alias
        self.stopped = False
        self.consume_connect = None
        self.publish_connect = None
        self.storage = {}
        self.correlation_id = gen_curr_request_id()
        self.connect_options = connect_options or {}
        self.consume_options = consume_options or {}
        self.publish_options = publish_options or {}
        super(AMQPRpcProxy, self).__init__(**kwargs)

    @staticmethod
    def get_exchange() -> Exchange:
        """ 消费者使用交换机 """
        exchange_name = DEFAULT_KOMBU_AMQP_REPLY_EXCHANGE_NAME
        return Exchange(name=exchange_name, type='direct', auto_delete=True)

    def get_routing_key(self) -> t.Text:
        """ 消费者绑定路由键 """
        exchange_name = DEFAULT_KOMBU_AMQP_REPLY_EXCHANGE_NAME
        return f'{exchange_name}.{self.object_name}.{self.correlation_id}'

    def get_queue(self) -> Queue:
        """ 消费者使用的队列 """
        exchange = self.get_exchange()
        routing_key = self.get_routing_key()
        exchange_name = DEFAULT_KOMBU_AMQP_REPLY_EXCHANGE_NAME
        queue_name = f'{exchange_name}.{self.object_name}.{self.correlation_id}'
        return Queue(name=queue_name, exchange=exchange, routing_key=routing_key, auto_delete=True)

    def setup(self) -> None:
        """ 生命周期 - 载入阶段

        @return: None
        """
        connect_options = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.connect_options', {})
        # 防止YAML中声明值为None
        self.connect_options = (connect_options or {}) | self.connect_options
        self.connect_options.setdefault('heartbeat ', DEFAULT_KOMBU_AMQP_HEARTBEAT)
        self.consume_connect = Connection(**self.connect_options)
        self.publish_connect = Connection(**self.connect_options)
        consume_options = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.consume_options', {})
        consume_options.update({'callbacks': [self.handle_request]})
        consume_options.update({'queues': [self.get_queue()]})
        # 防止YAML中声明值为None
        self.consume_options = (consume_options or {}) | self.consume_options
        self.consume_options.update({'no_ack': True})
        publish_options = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.publish_options', {})
        # 防止YAML中声明值为None
        self.publish_options = (publish_options or {}) | self.publish_options
        self.publish_options.setdefault('serializer', 'json')
        self.producer.reg_extension(self)

    def stop(self) -> None:
        """ 生命周期 - 停止阶段

        @return: None
        """
        self.stopped = True
        self.producer.del_extension(self)
        self.consume_connect and self.consume_connect.release()
        self.publish_connect and self.publish_connect.release()

    def handle_request(self, body: t.Any, message: Message) -> None:
        """ 处理工作请求

        @return: t.Tuple
        """
        correlation_id = message.properties.get('correlation_id', None)
        correlation_id and self.storage.update({correlation_id: (body, message)})

    def get_instance(self, context: WorkerContext) -> t.Any:
        """ 获取注入对象

        @param context: 上下文对象
        @return: t.Any
        """
        return AMQPRpcRequest(self, context=context)
