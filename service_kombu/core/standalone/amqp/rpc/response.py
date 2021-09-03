#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import time
import eventlet
import typing as t

from logging import getLogger
from service_kombu.exception import ReachTiming
from service_core.core.decorator import AsLazyProperty
from service_core.exchelper import gen_exception_from_result

logger = getLogger(__name__)


class AMQPRpcResponse(object):
    """ AMQP RPC响应处理器 """

    def __init__(
            self,
            proxy: 'AMQPRpcStandaloneProxy',
            correlation_id: t.Text,
            timeout: t.Optional[int] = None
    ) -> None:
        """ 初始化实例

        @param proxy: 代理对象
        @param correlation_id: 关联ID
        @param timeout: 等待响应超时时间
        """
        self.proxy = proxy
        self.timeout = timeout or 1
        self.correlation_id = correlation_id

    @AsLazyProperty
    def result(self) -> t.Any:
        """ 获取执行结果 """
        original = self.correlation_id
        stop_time = time.time() + self.timeout
        while self.correlation_id not in self.proxy.storage:
            if self.proxy.stopped:
                break
            if time.time() < stop_time:
                eventlet.sleep(0.01)
            else:
                timeunit = 'seconds' if self.timeout > 1 else 'second'
                errormsg = f'reach timeout({self.timeout} {timeunit})'
                raise ReachTiming(errormsg=errormsg, original=original)
        if self.proxy.stopped: return
        body, message = self.proxy.storage.pop(self.correlation_id)
        if body['errs'] is None: return body['data'], message
        raise gen_exception_from_result(body['errs'])
