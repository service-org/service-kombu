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
        self.stopped = False
        self.consumers = []
        self.connection = None
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
        self.connection = Connection(**self.connect_options)
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
        self.connection and self.connection.release()

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
    ) -> t.Generator:
        """ 开始接收回复

        @param correlation_id: 关联ID
        @param connect_options: 连接参数
        @param consume_options: 消费参数
        @return: None
        """
        consumer, connection_loss, queue_declared = None, False, False
        while not self.stopped or correlation_id not in self.storage:
            try:
                if connection_loss is True:
                    logger.debug(f'{self} connection loss, start reconnecting')
                    self.connection = Connection(**connect_options)
                    logger.debug(f'{self} connection lose, reconnect success')
                    connection_loss = False
                consumer = Consumer(self.connection, **consume_options)
                consumer.consume()
                self.consumers.append(consumer)
                logger.debug(f'{self} start consuming with {consume_options}')
                while not self.stopped or correlation_id not in self.storage:
                    self.connection.drain_events(timeout=1)
                    # 必须在对端发送响应消息前创建并启动监听响应队列获取消息
                    if not queue_declared: yield
                    queue_declared = True
                # 优雅处理如ctrl + c, sys.exit, kill thread时的异常
            except (KeyboardInterrupt, SystemExit, GreenletExit):
                break
                # 优雅处理ConnectionError等连接异常断开异常会去自动重试
            except (ConnectionError, ChannelError, OperationalError, InconsistencyError):
                connection_loss = True
                # 如果之前建立过连接,暂不关心当前连接状态强制关闭掉当前连接
                self.connection and AsFriendlyFunc(self.connection.release)()
                logger.error(f'connection error while consumer consume', exc_info=True)
                eventlet.sleep(2)
                # 上面设置了drain_events等待事件的时间,这里忽略超时异常
            except (socket.timeout,):
                continue
            except:
                # 应该避免其它未知异常中断当前消费者导致任务无法被及时消费
                logger.error(f'unexpected error while consumer consume', exc_info=True)
                eventlet.sleep(1)
            finally:
                queue_declared = True
                consumer in self.consumers and self.consumers.remove(consumer)

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
        consume = self._run_consume(correlation_id, connect_options, consume_options)
        Publisher(self.connection, context=context, **self.publish_options).publish(
            body, routing_key=target,
            reply_to=reply_queue.name,
            exchange=target_exchange,
            correlation_id=correlation_id, **kwargs
        )
        timeout = kwargs.get('timeout', sys.maxsize)
        unit = 'seconds' if timeout > 1 else 'second'
        errs = f'reach timeout({timeout} {unit})'
        exception = ReachTiming(errormsg=errs, original=target)
        self.consume_options.update({'callbacks': [self.handle_request]})
        self.consume_options.update({'queues': [reply_queue]})
        timer = eventlet.Timeout(timeout, exception)
        try:
            next(consume)
            return self._get_results(correlation_id)
        finally:
            timer.cancel()
