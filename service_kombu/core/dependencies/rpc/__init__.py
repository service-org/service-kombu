#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from kombu import Queue
from kombu import Exchange
from logging import getLogger
from kombu.message import Message
from service_kombu.core.client import AMQPClient
from service_core.core.context import WorkerContext
from service_kombu.constants import KOMBU_CONFIG_KEY
from service_core.core.as_helper import gen_curr_request_id
from service_core.core.service.dependency import Dependency
from service_kombu.core.convert import from_context_to_headers
from service_kombu.constants import DEFAULT_KOMBU_AMQP_HEARTBEAT
from service_kombu.constants import DEFAULT_KOMBU_AMQP_HEADERS_MAPPING
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
            storage_buffer: t.Optional[int] = None,
            headers_mapping: t.Optional[t.Dict[t.Text, t.Any]] = None,
            connect_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            consume_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            publish_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            **kwargs: t.Any
    ) -> None:
        """ 初始化实例

        @param alias: 配置别名
        @param storage_buffer: 缓存大小
        @param connect_options: 连接配置
        @param consume_options: 消费配置
        @param publish_options: 发布配置
        @param kwargs: 其它配置
        """
        self.alias = alias
        self.stopped = False
        self.consume_connect = None
        self.publish_connect = None
        self.storage = {'_': []}
        self.storage_buffer = storage_buffer or 100
        self.correlation_id = gen_curr_request_id()
        self.headers_mapping = headers_mapping or {}
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
        exchange_name = DEFAULT_KOMBU_AMQP_REPLY_EXCHANGE_NAME
        exchange, routing_key = self.get_exchange(), self.get_routing_key()
        queue_name = f'{exchange_name}.{self.object_name}.{self.correlation_id}'
        return Queue(name=queue_name, exchange=exchange, routing_key=routing_key, durable=False, auto_delete=True)

    def setup(self) -> None:
        """ 生命周期 - 载入阶段

        @return: None
        """
        headers_mapping = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.headers_mapping', {})
        # 防止YAML中声明值为None
        headers_mapping = DEFAULT_KOMBU_AMQP_HEADERS_MAPPING | (headers_mapping or {})
        self.headers_mapping = (headers_mapping or {}) | self.headers_mapping
        connect_options = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.connect_options', {})
        # 防止YAML中声明值为None
        self.connect_options = (connect_options or {}) | self.connect_options
        self.connect_options.setdefault('heartbeat ', DEFAULT_KOMBU_AMQP_HEARTBEAT)
        self.consume_connect = AMQPClient(**self.connect_options)
        self.publish_connect = AMQPClient(**self.connect_options)
        consume_options = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.consume_options', {})
        # 防止YAML中声明值为None
        self.consume_options = (consume_options or {}) | self.consume_options
        self.consume_options.update({'no_ack': True})
        self.consume_options.update({'auto_declare': True})
        self.consume_options.update({'callbacks': [self.handle_request]})
        self.consume_options.update({'queues': [self.get_queue()]})
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

    def get_instance(self, context: WorkerContext) -> t.Any:
        """ 获取注入对象

        @param context: 上下文对象
        @return: t.Any
        """
        headers = from_context_to_headers(context.data, mapping=self.headers_mapping)
        return AMQPRpcRequest(self, headers=headers)
