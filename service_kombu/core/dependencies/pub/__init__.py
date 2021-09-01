#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t


from service_kombu.core.publish import Publisher
from service_kombu.core.connect import Connection
from service_core.core.context import WorkerContext
from service_kombu.constants import KOMBU_CONFIG_KEY
from service_core.core.service.dependency import Dependency
from service_kombu.constants import DEFAULT_KOMBU_AMQP_HEARTBEAT


class AMQPPubProducer(Dependency):
    """ AMQP消息发布生产者类 """

    name = 'AMQPPubProducer'

    def __init__(
            self,
            alias: t.Text,
            connect_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            publish_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            **kwargs: t.Text
    ) -> None:
        """ 初始化实例

        @param alias: 配置别名
        @param connect_options: 连接配置
        @param publish_options: 发布配置
        @param kwargs: 其它配置
        """
        self.alias = alias
        self.connection = None
        self.connect_options = connect_options or {}
        self.publish_options = publish_options or {}
        super(AMQPPubProducer, self).__init__(**kwargs)

    def setup(self) -> None:
        """ 生命周期 - 载入阶段

        @return: None
        """
        connect_options = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.connect_options', {})
        # 防止YAML中声明值为None
        self.connect_options = (connect_options or {}) | self.connect_options
        self.connect_options.setdefault('heartbeat ', DEFAULT_KOMBU_AMQP_HEARTBEAT)
        self.connection = Connection(**self.connect_options)
        publish_options = self.container.config.get(f'{KOMBU_CONFIG_KEY}.{self.alias}.publish_options', {})
        # 防止YAML中声明值为None
        self.publish_options = (publish_options or {}) | self.publish_options
        self.publish_options.setdefault('serializer', 'json')

    def stop(self) -> None:
        """ 生命周期 - 停止阶段

        @return: None
        """
        self.connection and self.connection.release()

    def get_instance(self, context: WorkerContext) -> t.Any:
        """ 获取注入对象

        @param context: 上下文对象
        @return: t.Any
        """
        return Publisher(self.connection, context=context,  **self.publish_options)
