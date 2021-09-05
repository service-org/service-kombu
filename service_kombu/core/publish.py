#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from logging import getLogger
from kombu import Producer as BaseProducer
from service_core.core.context import WorkerContext
from service_kombu.core.convert import from_context_to_headers
from service_kombu.constants import DEFAULT_KOMBU_AMQP_HEADERS_MAPPING


logger = getLogger(__name__)

class Publisher(BaseProducer):
    """ AMQP通用发送类 """

    def __init__(self, *args: t.Any, context: t.Optional[WorkerContext] = None, **kwargs: t.Text) -> None:
        """ 初始化实例

        @param args: 位置参数
        @param context: 上下文
        @param kwargs: 命名参数
        """
        self.context = context
        super(Publisher, self).__init__(*args, **kwargs)

    def publish(self, body: t.Any, **kwargs: t.Any) -> None:
        """ 发布消息

        @param body: 发送内容
        @param kwargs: 发布参数
        @return: None
        """
        headers = kwargs.get('headers', {})
        context = {} if self.context is None else self.context.data
        mapping = DEFAULT_KOMBU_AMQP_HEADERS_MAPPING
        context_headers = from_context_to_headers(context, mapping)
        headers.update(context_headers)
        kwargs['headers'] = headers
        # 防止开启心跳后超时被服务端主动踢下线
        kwargs.setdefault('retry', True)
        logger.debug(f'publish message: {body} with {kwargs}')
        return super(Publisher, self).publish(body, **kwargs)
