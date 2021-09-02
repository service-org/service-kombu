#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from service_core.cli.subctxs import BaseContext
from service_core.core.configure import Configure

from .sub import AMQPSubClient
from .pub import AMQPPubClient
from .rpc import AMQPRpcClient


class AMQP(BaseContext):
    """ 用于调试AMQP接口 """

    name: t.Text = 'amqp'

    def __init__(self, config: Configure) -> None:
        """ 初始化实例

        @param config: 配置对象
        """
        super(AMQP, self).__init__(config)
        self.sub = AMQPSubClient(config=config)
        self.pub = AMQPPubClient(config=config)
        self.rpc = AMQPRpcClient(config=config)
