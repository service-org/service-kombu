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
from service_green.core.green import cjson
from eventlet.greenthread import GreenThread
from service_kombu.core.publish import Publisher
from service_kombu.core.connect import Connection
from service_core.core.context import WorkerContext
from service_kombu.constants import KOMBU_CONFIG_KEY
from service_core.core.decorator import AsLazyProperty
from service_core.core.service.entrypoint import Entrypoint
from service_core.exchelper import gen_exception_description
from service_kombu.core.convert import from_headers_to_context
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
        self.publisher = None
        self.connection = None
        self.connect_options = connect_options or {}
        self.consume_options = consume_options or {}
        self.publish_options = publish_options or {}
        super(AMQPRpcConsumer, self).__init__(**kwargs)

    @AsLazyProperty
    def routing_key(self):
        """ 消费者绑定路由键 """
        return f'{self.container.service.name}.{self.object_name}'

    @AsLazyProperty
    def queue(self):
        """ 消费者使用的队列 """
        name = f'{self.container.service.name}.{self.object_name}'
        return Queue(name=name, exchange=self.exchange, routing_key=self.routing_key)

    @AsLazyProperty
    def exchange(self):
        """ 消费者使用交换机 """
        name = f'{self.container.service.name}'
        return Exchange(name=name, type='direct', auto_delete=True)

    def setup(self) -> None:
        """ 生命周期 - 载入阶段

        @return: None
        """
        connect_options = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.connect_options', {})
        # 防止YAML中声明值为None
        self.connect_options = (connect_options or {}) | self.connect_options
        self.connect_options.setdefault('heartbeat ', DEFAULT_KOMBU_AMQP_HEARTBEAT)
        self.connection = Connection(**self.connect_options)
        consume_options = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.consume_options', {})
        # 防止YAML中声明值为None
        self.consume_options = (consume_options or {}) | self.consume_options
        self.consume_options.setdefault('callbacks', [self.handle_request])
        self.consume_options.setdefault('queues', [self.queue])
        publish_options = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.publish_options', {})
        # 防止YAML中声明值为None
        self.publish_options = (publish_options or {}) | self.publish_options
        self.publish_options.setdefault('serializer', 'json')
        self.publisher = Publisher(self.connection, **self.publish_options)
        self.producer.reg_extension(self)

    def stop(self) -> None:
        """ 生命周期 - 停止阶段

        @return: None
        """
        self.producer.del_extension(self)
        self.connection and self.connection.release()

    def kill(self) -> None:
        """ 生命周期 - 强杀阶段

        @return: None
        """
        self.producer.del_extension(self)

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

    def handle_request(self, body: t.Any, message: Message) -> t.Tuple:
        """ 处理工作请求

        @return: t.Tuple
        """
        event = Event()
        tid = f'{self}.self_handle_request'
        args, kwargs = (body, message), {}
        context = from_headers_to_context(message.headers, DEFAULT_KOMBU_AMQP_HEADERS_MAPPING)
        gt = self.container.spawn_worker_thread(self, args=args, kwargs=kwargs, context=context, tid=tid)
        gt.link(self._link_results, event)
        # 注意: 协程异常会导致收不到event最终内存溢出!
        context, results, excinfo = event.wait()
        # 注意: 不管成功或失败都尝试去自动确认这个消息!
        message.ack()
        return (
            self.handle_result(context, results)
            if excinfo is None else
            self.handle_errors(context, excinfo)
        )

    def send_response(self, body: t.Optional[t.Text, t.Dict[t.Text, t.Any]], **kwargs: t.Any) -> None:
        """ 发送响应消息

        @param body: 消息内容
        @param kwargs: 命名参数
        @return: None
        """
        body = cjson.dumps(body) if isinstance(body, dict) else body

    def handle_result(self, context: WorkerContext, results: t.Any) -> t.Any:
        """ 处理正常结果

        @param context: 上下文对象
        @param results: 结果对象
        @return: t.Any
        """
        errs, call_id = None, context.worker_request_id
        return cjson.dumps({'code': 200, 'errs': None, 'data': results, 'call_id': call_id})

    def handle_errors(self, context: WorkerContext, excinfo: t.Tuple) -> t.Any:
        """ 处理异常结果

        @param context: 上下文对象
        @param excinfo: 异常对象
        @return: t.Any
        """
        exc_type, exc_value, exc_trace = excinfo
        data, call_id = None, context.worker_request_id
        errs = gen_exception_description(exc_value)
        return cjson.dumps({'code': 500, 'errs': errs, 'data': None, 'call_id': call_id})
