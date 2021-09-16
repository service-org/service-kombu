#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from kombu import Producer
from logging import getLogger

logger = getLogger(__name__)


class Publisher(Producer):
    """ AMQP通用发送类 """

    def __init__(self, *args: t.Any, headers: t.Optional[t.Dict[t.Text, t.Any]] = None, **kwargs: t.Any) -> None:
        """ 初始化实例

        @param args: 位置参数
        @param headers: 额外头部
        @param kwargs: 命名参数
        """
        self.headers = headers or {}
        super(Publisher, self).__init__(*args, **kwargs)

    def publish(self, body: t.Any, **kwargs: t.Any) -> None:
        """ 发布消息

        @param body: 发送内容
        @param kwargs: 发布参数
        @return: None
        """
        kwargs.get('headers', {}).update(self.headers)
        # 防止开启心跳后超时被服务端主动踢下线
        kwargs.setdefault('retry', True)
        logger.debug(f'publish message: {body} with {kwargs}')
        return super(Publisher, self).publish(body, **kwargs)
