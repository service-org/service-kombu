#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from kombu import Exchange
from logging import getLogger
from service_kombu.core.publish import Publisher
from service_core.core.as_helper import gen_curr_request_id

from .response import AMQPRpcResponse

logger = getLogger(__name__)


class AMQPRpcRequest(object):
    """ AMQP RPC请求处理器 """

    def __init__(self, proxy: 'AMQPRpcStandaloneProxy') -> None:
        """ 初始化实例
        
        @param proxy: 代理对象
        """
        self.proxy = proxy

    @staticmethod
    def get_target_exchange(name: t.Text) -> Exchange:
        """ 目标的使用交换机 """
        return Exchange(name=name, type='direct', auto_delete=True)

    def send_request(self, target: t.Text, body: t.Any, **kwargs: t.Any) -> AMQPRpcResponse:
        """ 发送RPC请求

        @param target: 目标节点
        @param body: 发送内容
        @param kwargs: 其它参数
        @return: t.Any
        """
        correlation_id = f'{target}.{gen_curr_request_id()}'
        reply_queue = self.proxy.get_queue()
        target_exchange = self.get_target_exchange(target.split('.', 1)[0])
        publisher = Publisher(
            self.proxy.publish_connect,
            **self.proxy.publish_options
        )
        publisher.publish(
            body, routing_key=target,
            reply_to=reply_queue.name,
            exchange=target_exchange,
            correlation_id=correlation_id, **kwargs
        )
        timeout = kwargs.get('timeout', 1)
        return AMQPRpcResponse(self.proxy, correlation_id, timeout=timeout)
