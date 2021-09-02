#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import sys
import time
import eventlet
import typing as t

from logging import getLogger
from service_green.core.green import cjson
from service_kombu.exception import ReachTiming
from service_core.core.decorator import AsLazyProperty
from service_core.core.service.dependency import Dependency
from service_core.exchelper import gen_exception_from_result

logger = getLogger(__name__)


class AMQPRpcResponse(object):
    """ AMQP RPC响应处理器 """

    def __init__(self, dependency: Dependency, correlation_id: t.Text, timeout: t.Optional[int] = None) -> None:
        """ 初始化实例

        @param dependency: 依赖对象
        @param correlation_id: 关联ID
        @param timeout: 等待响应超时时间
        """
        self.dependency = dependency
        self.correlation_id = correlation_id
        self.timeout = timeout or sys.maxsize

    @AsLazyProperty
    def result(self) -> t.Any:
        """ 获取执行结果 """
        original = self.correlation_id
        stop_time = time.time() + self.timeout
        while self.correlation_id not in self.dependency.storage:
            if self.dependency.stopped:
                break
            if time.time() < stop_time:
                eventlet.sleep(0.01)
            else:
                timeunit = 'seconds' if self.timeout > 1 else 'second'
                errormsg = f'reach timeout({self.timeout} {timeunit})'
                raise ReachTiming(errormsg=errormsg, original=original)
        if self.dependency.stopped: return
        body, message = self.dependency.storage.pop(self.correlation_id)
        data = cjson.loads(body)
        if data['errs'] is None: return data['data'], message
        raise gen_exception_from_result(data['errs'])
