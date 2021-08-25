#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from service_core.cli.subctxs import BaseContext
from service_core.core.configure import Configure
from service_consul.core.proxy import AMQPRpcProxy


class AMQPRpcClient(BaseContext):
    """ 用于调试RPC接口 """

    name: t.Text = 'rpc'

    def __init__(self, config: Configure) -> None:
        """ 初始化实例

        @param config: 配置对象
        """
        super(AMQPRpcClient, self).__init__(config)
        self.proxy = AMQPRpcProxy(config=config)
