#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

from .sub.consumer import AMQPSubConsumer
from .rpc.consumer import AMQPRpcConsumer

sub = AMQPSubConsumer.as_decorators
rpc = AMQPRpcConsumer.as_decorators
