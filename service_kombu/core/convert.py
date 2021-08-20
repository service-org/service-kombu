#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t


def from_context_to_headers(context: t.Dict, mapping: t.Dict) -> t.Dict:
    """ 从上下文提取并生成头部信息

    @param context: 上下文信息
    @param mapping: 映射转换器
    @return: t.Dict
    """
    # 将上下文转换为对应请求头,可兼容不同的链路追踪协议
    current_headers = {}
    for k, v in context.items():
        k = mapping[k] if k in mapping else k
        current_headers[k] = v
    return current_headers


def from_headers_to_context(headers: t.Dict, mapping: t.Dict) -> t.Dict:
    """ 从头部信息提取并生成上下文

    @param headers: 头部信息
    @param mapping: 映射转换器
    @return: t.Dict
    """
    # 将请求头转换为对应上下文,可兼容不同的链路追踪协议
    current_context = {}
    for k, v in headers.items():
        k = mapping[k] if k in mapping else k
        current_context[k] = v
    return current_context
