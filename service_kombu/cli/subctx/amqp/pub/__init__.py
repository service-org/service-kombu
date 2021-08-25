#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from service_core.cli.subctxs import BaseContext
from service_core.core.configure import Configure
from service_consul.core.proxy import AMQPPubProxy


class AMQPPubClient(BaseContext):
    """ 用于调试发布接口 """

    name: t.Text = 'pub'

    def __init__(self, config: Configure) -> None:
        """ 初始化实例

        @param config: 配置对象
        """
        super(AMQPPubClient, self).__init__()
        self.proxy = AMQPPubProxy(config=config)
