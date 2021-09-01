#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

from service_core.exception import RemoteError


class ReachTiming(RemoteError):
    """ 任务已执行超时 """
    pass
