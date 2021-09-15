#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from service_kombu.core.publish import Publisher
from service_kombu.core.client import AMQPClient


class AMQPPubStandaloneProxy(object):
    """ AMQP PUB发布者代理 """

    def __init__(self, config: t.Dict[t.Text, t.Any]) -> None:
        """ 初始化实例

        @param config: 配置字典
        """
        self.connect_options = config.get('connect_options', {})
        self.publish_options = config.get('publish_options', {})
        self.publish_connect = AMQPClient(**self.connect_options)

    def as_inst(self) -> Publisher:
        """ 创建时逻辑 """
        return Publisher(self.publish_connect, **self.publish_options)

    def release(self) -> None:
        """ 销毁时逻辑 """
        self.publish_connect and self.publish_connect.release()

    def __enter__(self) -> Publisher:
        """ 创建时回调 """
        return self.as_inst()

    def __exit__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """ 销毁时回调 """
        return self.release()
