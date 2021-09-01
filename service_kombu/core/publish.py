#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from kombu import Producer as BaseProducer
from service_core.core.context import WorkerContext
from service_kombu.core.convert import from_context_to_headers
from service_kombu.constants import DEFAULT_KOMBU_AMQP_HEADERS_MAPPING


class Publisher(BaseProducer):
    """ AMQP通用发送类 """

    def __init__(self, *args: t.Any, context: WorkerContext, **kwargs: t.Text) -> None:
        """ 初始化实例

        @param args: 位置参数
        @param context: 上下文
        @param kwargs: 命名参数
        """
        self.context = context
        super(Publisher, self).__init__(*args, **kwargs)

    def publish(self, *args: t.Any, **kwargs: t.Any) -> None:
        """ 发布消息

        @param args  : 位置参数
        @param kwargs: 命名参数
        @return: None
        """
        headers = kwargs.get('headers', {})
        context = self.context.data
        mapping = DEFAULT_KOMBU_AMQP_HEADERS_MAPPING
        context_headers = from_context_to_headers(context, mapping)
        headers.update(context_headers)
        kwargs['headers'] = headers
        # 防止开启心跳后超时被服务端主动踢下线
        kwargs.setdefault('retry', True)
        return super(Publisher, self).publish(*args, **kwargs)
