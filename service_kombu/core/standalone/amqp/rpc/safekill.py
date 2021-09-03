#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import ctypes
import typing as t

from threading import Thread


def safe_kill_thread(thread: Thread, exception: t.Type[BaseException]) -> int:
    """ 安全杀死线程

    @param thread: 线程对象
    @param exception: 异常类型
    @return: int
    """
    thread_ident = ctypes.c_long(thread.ident)
    thread_error = ctypes.py_object(exception)
    return ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_ident, thread_error)
