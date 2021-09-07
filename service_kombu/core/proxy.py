#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from kombu import Consumer
from kombu import Producer
from kombu import Connection
from service_core.core.configure import Configure
from service_kombu.constants import KOMBU_CONFIG_KEY
from service_kombu.core.standalone.amqp.rpc import AMQPRpcStandaloneProxy

from .standalone.amqp.rpc.requests import AMQPRpcRequest


class AMQPSubProxy(object):
    """ 消息订阅代理类 """

    def __init__(
            self,
            config: Configure,
            connect_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            consume_options: t.Optional[t.Dict[t.Text, t.Any]] = None
    ) -> None:
        """ 初始化实例

        @param config: 配置对象
        @param connect_options: 连接配置
        @param consume_options: 消费配置
        """
        self.config = config
        self.connect_options = connect_options or {}
        self.consume_options = consume_options or {}

    def __call__(
            self,
            alias: t.Text,
            connect_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            consume_options: t.Optional[t.Dict[t.Text, t.Any]] = None
    ) -> Consumer:
        """ 代理可调用

        @param alias: 配置别名
        @param connect_options: 连接配置
        @param consume_options: 消费配置
        @return: Consumer
        """
        cur_connect_options = self.connect_options
        cur_consume_options = self.consume_options
        # 调用时传递的参数配置优先级最高
        cur_connect_options.update(connect_options or {})
        cur_consume_options.update(consume_options or {})
        cfg_connect_options = self.config.get(f'{KOMBU_CONFIG_KEY}.{alias}.connect_options', default={})
        cfg_consume_options = self.config.get(f'{KOMBU_CONFIG_KEY}.{alias}.consume_options', default={})
        # 调用时传递的参数配置优先级最高
        cfg_connect_options.update(cur_connect_options)
        cfg_consume_options.update(cur_consume_options)
        connection = Connection(**cfg_connect_options)
        return Consumer(connection, **cfg_consume_options)


class AMQPPubProxy(object):
    """ 消息发布代理类 """

    def __init__(
            self,
            config: Configure,
            connect_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            consume_options: t.Optional[t.Dict[t.Text, t.Any]] = None
    ) -> None:
        """ 初始化实例

        @param config: 配置对象
        @param connect_options: 连接配置
        @param consume_options: 消费配置
        """
        self.config = config
        self.connect_options = connect_options or {}
        self.consume_options = consume_options or {}

    def __call__(
            self,
            alias: t.Text,
            connect_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            publish_options: t.Optional[t.Dict[t.Text, t.Any]] = None
    ) -> Producer:
        """ 代理可调用

        @param alias: 配置别名
        @param connect_options: 连接配置
        @param publish_options: 发布配置
        @return: Producer
        """
        cur_connect_options = self.connect_options
        cur_publish_options = self.consume_options
        # 调用时传递的参数配置优先级最高
        cur_connect_options.update(connect_options or {})
        cur_publish_options.update(publish_options or {})
        cfg_connect_options = self.config.get(f'{KOMBU_CONFIG_KEY}.{alias}.connect_options', default={})
        cfg_publish_options = self.config.get(f'{KOMBU_CONFIG_KEY}.{alias}.publish_options', default={})
        # 调用时传递的参数配置优先级最高
        cfg_connect_options.update(cur_connect_options)
        cfg_publish_options.update(cur_publish_options)
        connection = Connection(**cfg_connect_options)
        return Producer(connection, **cfg_publish_options)


class AMQPRpcProxy(object):
    """ RPC请求代理类 """

    def __init__(
            self,
            config: Configure,
            connect_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            consume_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            publish_options: t.Optional[t.Dict[t.Text, t.Any]] = None
    ) -> None:
        """ 初始化实例

        @param config: 配置对象
        @param connect_options: 连接配置
        @param consume_options: 消费配置
        @param publish_options: 发布配置
        """
        self.config = config
        self.connect_options = connect_options or {}
        self.consume_options = consume_options or {}
        self.publish_options = publish_options or {}

    def __call__(
            self,
            alias: t.Text,
            connect_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            consume_options: t.Optional[t.Dict[t.Text, t.Any]] = None,
            publish_options: t.Optional[t.Dict[t.Text, t.Any]] = None
    ) -> AMQPRpcRequest:
        """ 代理可调用

        @param alias: 配置别名
        @param connect_options: 连接配置
        @param consume_options: 消费配置
        @param publish_options: 发布配置
        @return: AMQPRpcRequest
        """
        cur_connect_options = self.connect_options
        cur_consume_options = self.consume_options
        cur_publish_options = self.publish_options
        # 调用时传递的参数配置优先级最高
        cur_connect_options.update(connect_options or {})
        cur_consume_options.update(consume_options or {})
        cur_publish_options.update(publish_options or {})
        cfg_connect_options = self.config.get(f'{KOMBU_CONFIG_KEY}.{alias}.connect_options', default={})
        cfg_consume_options = self.config.get(f'{KOMBU_CONFIG_KEY}.{alias}.consume_options', default={})
        cfg_publish_options = self.config.get(f'{KOMBU_CONFIG_KEY}.{alias}.publish_options', default={})
        # 调用时传递的参数配置优先级最高
        cfg_connect_options.update(cur_connect_options)
        cfg_consume_options.update(cur_consume_options)
        cfg_publish_options.update(cur_publish_options)
        config = {
            'connect_options': cfg_connect_options,
            'consume_options': cfg_consume_options,
            'publish_options': cfg_publish_options,
        }
        # 消费者线程将与启动shell共生死
        return AMQPRpcStandaloneProxy(config=config, drain_events_timeout=None).as_inst()
