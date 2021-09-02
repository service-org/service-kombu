#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from service_core.cli.subctxs import BaseContext
from service_core.core.configure import Configure
from service_kombu.core.proxy import AMQPSubProxy


class AMQPSubClient(BaseContext):
    """ 用于调试订阅接口 """

    name: t.Text = 'sub'

    def __init__(self, config: Configure) -> None:
        """ 初始化实例

        @param config: 配置对象
        """
        super(AMQPSubClient, self).__init__(config)
        self.proxy = AMQPSubProxy(config=config)
