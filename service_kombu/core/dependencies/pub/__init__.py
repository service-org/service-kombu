#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import eventlet
import typing as t

from service_kombu.core.publish import Publisher
from service_kombu.core.client import AMQPClient
from service_kombu.constants import KOMBU_CONFIG_KEY
from service_core.core.service.dependency import Dependency
from service_kombu.core.convert import from_context_to_headers
from service_kombu.constants import DEFAULT_KOMBU_AMQP_HEARTBEAT
from service_kombu.constants import DEFAULT_KOMBU_AMQP_HEADERS_MAPPING


class AMQPPubProducer(Dependency):
    """ AMQP消息发布生产者类 """

    name = 'AMQPPubProducer'

    def __init__(
            self,
            alias: t.Text,
            headers_mapping: t.Optional[t.Dict[t.Text, t.Any]] = None,
            connect_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            publish_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            **kwargs: t.Any
    ) -> None:
        """ 初始化实例

        @param alias: 配置别名
        @param connect_options: 连接配置
        @param publish_options: 发布配置
        @param kwargs: 其它配置
        """
        self.alias = alias
        self.publish_connect = None
        self.headers_mapping = headers_mapping or {}
        self.connect_options = connect_options or {}
        self.publish_options = publish_options or {}
        super(AMQPPubProducer, self).__init__(**kwargs)

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
        self.publish_connect = AMQPClient(**self.connect_options)
        publish_options = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.publish_options', {})
        # 防止YAML中声明值为None
        self.publish_options = (publish_options or {}) | self.publish_options
        self.publish_options.setdefault('serializer', 'json')

    def stop(self) -> None:
        """ 生命周期 - 停止阶段

        @return: None
        """
        self.publish_connect and self.publish_connect.release()

    def get_client(self) -> Publisher:
        """ 获取一个独立的会话

        @return: Publisher
        """
        context = eventlet.getcurrent().context
        headers = from_context_to_headers(context.data, mapping=self.headers_mapping)
        return Publisher(self.publish_connect, headers=headers, **self.publish_options)
