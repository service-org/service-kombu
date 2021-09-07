#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import sys
import eventlet
import typing as t

from kombu import Queue
from kombu import Exchange
from logging import getLogger
from eventlet.event import Event
from kombu.message import Message
from eventlet.greenthread import GreenThread
from service_kombu.core.publish import Publisher
from service_kombu.core.connect import Connection
from service_core.core.context import WorkerContext
from service_kombu.constants import KOMBU_CONFIG_KEY
from service_core.core.service.entrypoint import Entrypoint
from service_core.exchelper import gen_exception_description
from service_kombu.core.convert import from_headers_to_context
from service_kombu.core.convert import from_context_to_headers
from service_kombu.constants import DEFAULT_KOMBU_AMQP_HEARTBEAT
from service_kombu.constants import DEFAULT_KOMBU_AMQP_HEADERS_MAPPING

from .producer import AMQPRpcProducer

logger = getLogger(__name__)


class AMQPRpcConsumer(Entrypoint):
    """ AMQP消息订阅消费者类 """

    name = 'AMQPRpcConsumer'

    producer = AMQPRpcProducer()

    def __init__(
            self,
            alias: t.Text,
            headers_mapping: t.Optional[t.Dict[t.Text, t.Any]] = None,
            connect_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            consume_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            publish_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            **kwargs: t.Text
    ) -> None:
        """ 初始化实例

        @param alias: 配置别名
        @param headers_mapping: 头部映射
        @param connect_options: 连接配置
        @param consume_options: 消费配置
        @param publish_options: 发布配置
        @param kwargs: 其它配置
        """
        self.alias = alias
        self.consume_connect = None
        self.publish_connect = None
        self.headers_mapping = headers_mapping or {}
        self.connect_options = connect_options or {}
        self.consume_options = consume_options or {}
        self.publish_options = publish_options or {}
        super(AMQPRpcConsumer, self).__init__(**kwargs)

    def get_exchange(self) -> Exchange:
        """ 消费者使用交换机 """
        exchange_name = self.container.service.name
        return Exchange(name=exchange_name, type='direct', auto_delete=True)

    def get_routing_key(self) -> t.Text:
        """ 消费者绑定路由键 """
        exchange_name = self.container.service.name
        return f'{exchange_name}.{self.object_name}'

    def get_queue(self) -> Queue:
        """ 消费者使用的队列 """
        exchange_name = self.container.service.name
        queue_name = f'{exchange_name}.{self.object_name}'
        exchange = self.get_exchange()
        routing_key = self.get_routing_key()
        return Queue(name=queue_name, exchange=exchange, routing_key=routing_key, durable=False, auto_delete=True)

    @staticmethod
    def get_target_exchange(name: t.Text) -> Exchange:
        """ 目标的使用交换机 """
        return Exchange(name=name, type='direct', auto_delete=True)

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
        self.consume_connect = Connection(**self.connect_options)
        self.publish_connect = Connection(**self.connect_options)
        consume_options = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.consume_options', {})
        # 防止YAML中声明值为None
        self.consume_options = (consume_options or {}) | self.consume_options
        # RPC请求成功失败都自动确认
        self.consume_options.update({'no_ack': True})
        self.consume_options.update({'callbacks': [self.handle_request]})
        self.consume_options.update({'queues': [self.get_queue()]})
        publish_options = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.publish_options', {})
        # 防止YAML中声明值为None
        self.publish_options = (publish_options or {}) | self.publish_options
        self.publish_options.update({'serializer': 'json'})
        self.producer.reg_extension(self)

    def stop(self) -> None:
        """ 生命周期 - 停止阶段

        @return: None
        """
        self.producer.del_extension(self)
        self.consume_connect and self.consume_connect.release()
        self.publish_connect and self.publish_connect.release()

    def kill(self) -> None:
        """ 生命周期 - 强杀阶段

        @return: None
        """
        self.producer.del_extension(self)

    @staticmethod
    def is_rpc_request(message: Message) -> bool:
        """ 是否是rpc请求

        @param message: 消息对象
        @return: bool
        """
        return (
                'reply_to' in message.properties
                and
                message.properties['reply_to']
                and
                'correlation_id' in message.properties
                and
                message.properties['correlation_id']
        )

    @staticmethod
    def _link_results(gt: GreenThread, event: Event) -> None:
        """ 等待执行结果

        @param gt: 协程对象
        @param event: 事件
        @return: None
        """
        # fix: 此协程异常会导致收不到event最终内存溢出!
        try:
            context, results, excinfo = gt.wait()
        except Exception:
            results, excinfo = None, sys.exc_info()
            context = eventlet.getcurrent().context
        event.send((context, results, excinfo))

    def handle_request(self, body: t.Any, message: Message) -> None:
        """ 处理工作请求

        @return: t.Tuple
        """
        # 如果是非RPC请求则记录非法请求然后强制返回!
        is_rpc_request = self.is_rpc_request(message)
        warns = f'got invalid rpc request data {body} with {message}, ignore'
        if not is_rpc_request: logger.warning(warns)
        if not is_rpc_request: return
        event = Event()
        tid = f'{self}.self_handle_request'
        args, kwargs = (body, message), {}
        context = from_headers_to_context(message.headers, self.headers_mapping)
        gt = self.container.spawn_worker_thread(self, args=args, kwargs=kwargs, context=context, tid=tid)
        gt.link(self._link_results, event)
        # 注意: 协程异常会导致收不到event最终内存溢出!
        context, results, excinfo = event.wait()
        return (
            self.handle_result(context, results, message)
            if excinfo is None else
            self.handle_errors(context, excinfo, message)
        )

    def send_response(
            self,
            body: t.Dict[t.Text, t.Any],
            context: WorkerContext,
            message: Message,
            **kwargs: t.Any
    ) -> None:
        """ 发送响应消息

        @param body: 消息内容
        @param context: 上下文对象
        @param message: 消息对象
        @param kwargs: 其它参数
        @return: None
        """
        reply_to = message.properties['reply_to']
        correlation_id = message.properties['correlation_id']
        target_exchange = self.get_target_exchange(reply_to.split('.', 1)[0])
        headers = from_context_to_headers(context.data, mapping=self.headers_mapping)
        Publisher(self.publish_connect, headers=headers, **self.publish_options).publish(
            body, exchange=target_exchange,
            correlation_id=correlation_id,
            routing_key=reply_to, **kwargs
        )

    def handle_result(self, context: WorkerContext, results: t.Any, message: Message) -> None:
        """ 处理正常结果

        @param context: 上下文对象
        @param results: 结果对象
        @param message: 消息对象
        @return: None
        """
        errs, call_id = None, context.worker_request_id
        body = {'code': 200, 'errs': None, 'data': results, 'call_id': call_id}
        self.send_response(body, context=context, message=message)

    def handle_errors(self, context: WorkerContext, excinfo: t.Tuple, message: Message) -> None:
        """ 处理异常结果

        @param context: 上下文对象
        @param excinfo: 异常对象
        @param message: 消息对象
        @return: None
        """
        exc_type, exc_value, exc_trace = excinfo
        data, call_id = None, context.worker_request_id
        errs = gen_exception_description(exc_value)
        body = {'code': 500, 'errs': errs, 'data': None, 'call_id': call_id}
        self.send_response(body, context=context, message=message)
