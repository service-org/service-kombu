#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

from kombu import Connection


class AMQPClient(Connection):
    """ AMQP通用连接类 """

    def connect(self) -> AMQPClient:
        """ Establish connection to server immediately.

        @return: AMQPClient
        """
        # 关闭默认重试,由消费者调度器自己实现
        return self._ensure_connection(
            max_retries=0, reraise_as_library_errors=False
        )
