#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import sys
import socket
import eventlet
import typing as t

from kombu import Queue
from kombu import Exchange
from kombu import Consumer
from logging import getLogger
from kombu.message import Message
from greenlet import GreenletExit
from kombu.serialization import registry
from kombu.exceptions import ChannelError
from kombu.exceptions import ConnectionError
from kombu.exceptions import OperationalError
from kombu.exceptions import InconsistencyError
from service_kombu.exception import ReachTiming
from service_kombu.core.publish import Publisher
from service_kombu.core.connect import Connection
from service_core.core.spawning import SpawningProxy
from service_kombu.constants import KOMBU_CONFIG_KEY
from service_core.core.decorator import AsFriendlyFunc
from service_core.core.as_helper import gen_curr_request_id
from service_core.core.service.dependency import Dependency
from service_kombu.constants import DEFAULT_KOMBU_AMQP_HEARTBEAT
from service_kombu.constants import DEFAULT_KOMBU_AMQP_REPLY_EXCHANGE_NAME

logger = getLogger(__name__)


class AMQPRpcProxy(Dependency):
    """ AMQP RPC请求代理类 """

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
        self.gt_list = []
        self.stopped = False
        self.consumers = []
        self.consume_connect = None
        self.publish_connect = None
        self.storage = {}
        self.connect_options = connect_options or {}
        self.consume_options = consume_options or {}
        self.publish_options = publish_options or {}
        super(AMQPRpcProxy, self).__init__(**kwargs)

    @staticmethod
    def get_exchange() -> Exchange:
        """ 消费者使用交换机 """
        exchange_name = DEFAULT_KOMBU_AMQP_REPLY_EXCHANGE_NAME
        return Exchange(name=exchange_name, type='direct')

    def get_routing_key(self, correlation_id: t.Text) -> t.Text:
        """ 消费者绑定路由键 """
        exchange_name = DEFAULT_KOMBU_AMQP_REPLY_EXCHANGE_NAME
        return f'{exchange_name}.{self.object_name}.{correlation_id}'

    def get_queue(self, correlation_id: t.Text) -> Queue:
        """ 消费者使用的队列 """
        exchange = self.get_exchange()
        routing_key = self.get_routing_key(correlation_id)
        exchange_name = DEFAULT_KOMBU_AMQP_REPLY_EXCHANGE_NAME
        queue_name = f'{exchange_name}.{self.object_name}.{correlation_id}'
        return Queue(name=queue_name, exchange=exchange, routing_key=routing_key)

    @staticmethod
    def get_target_exchange(name: t.Text) -> Exchange:
        """ 目标的使用交换机 """
        return Exchange(name=name, type='direct', auto_delete=True)

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
        # 防止YAML中声明值为None
        self.consume_options = (consume_options or {}) | self.consume_options
        self.consume_options.update({'no_ack': True})
        publish_options = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.publish_options', {})
        # 防止YAML中声明值为None
        self.publish_options = (publish_options or {}) | self.publish_options
        self.publish_options.setdefault('serializer', 'json')

    def stop(self) -> None:
        """ 生命周期 - 停止阶段

        @return: None
        """
        self.stopped = True
        base_func = SpawningProxy(self.consumers).cancel
        wait_func = AsFriendlyFunc(base_func)
        self.consumers and wait_func()
        self.consume_connect and self.consume_connect.release()
        self.publish_connect and self.publish_connect.release()

    def kill(self) -> None:
        """ 生命周期 - 强杀阶段

        @return: None
        """
        self.stopped = True

    def handle_request(self, body: t.Any, message: Message) -> None:
        """ 处理工作请求

        @return: t.Tuple
        """
        correlation_id = message.properties.get('correlation_id', None)
        correlation_id and self.storage.update({correlation_id: (body, message)})

    def _run_consume(
            self,
            correlation_id: t.Text,
            connect_options: t.Dict[t.Text, t.Any],
            consume_options: t.Dict[t.Text, t.Any],
    ) -> None:
        """ 开始接收回复

        @param correlation_id: 关联ID
        @param connect_options: 连接参数
        @param consume_options: 消费参数
        @return: None
        """
        consumer = Consumer(self.consume_connect, **consume_options)
        consumer.consume()
        logger.debug(f'{self} start consuming with {consume_options}')
        while not self.stopped or correlation_id not in self.storage:
            self.consume_connect.drain_events()

    def _get_results(self, correlation_id: t.Text) -> t.Any:
        """ 获取对端的响应

        @param correlation_id: 关联ID
        @return: t.Any
        """
        if correlation_id in self.storage:
            body, message = self.storage.pop(correlation_id)
            print('=' * 100)
            print(type(body), body)
            print(type(message), message)
            print('=' * 100)

    def send_request(self, target: t.Text, body: t.Any, **kwargs: t.Any) -> t.Any:
        """ 发送RPC请求

        @param target: 目标节点
        @param body: 发送内容
        @param kwargs: 其它参数
        @return: t.Any
        """
        serializer = self.publish_options['serializer']
        body = registry.dumps(body, serializer=serializer)
        context = eventlet.getcurrent().context
        correlation_id = gen_curr_request_id()
        target_exchange = self.get_target_exchange(target.split('.', 1)[0])
        reply_queue = self.get_queue(correlation_id)
        connect_options = self.connect_options.copy()
        consume_options = self.consume_options.copy()
        consume_options.update({'callbacks': [self.handle_request]})
        consume_options.update({'queues': [reply_queue]})
        tid = f'{self}.self_run_consume'
        args = (correlation_id, connect_options, consume_options)
        gt = self.container.spawn_splits_thread(self._run_consume, args, {}, tid=tid)
        self.gt_list.append(gt)
        Publisher(self.publish_connect, context=context, **self.publish_options).publish(
            body, routing_key=target,
            reply_to=reply_queue.name,
            exchange=target_exchange,
            correlation_id=correlation_id, **kwargs
        )
        # timeout = kwargs.get('timeout', sys.maxsize)
        # unit = 'seconds' if timeout > 1 else 'second'
        # errs = f'reach timeout({timeout} {unit})'
        # exception = ReachTiming(errormsg=errs, original=target)
        # timer = eventlet.Timeout(timeout, exception)
        gt.wait()
        return self._get_results(correlation_id)
        # try:
        #     gt.wait()
        #     return self._get_results(correlation_id)
        # finally:
        #     timer.cancel()
