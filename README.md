# 运行环境

|system |python | 
|:------|:------|      
|cross platform |3.9.16|

# 组件安装

```shell
pip install -U service-kombu 
```

# 服务配置

> config.yaml

```yaml
CONTEXT:
  - service_kombu.cli.subctx.amqp:AMQP
KOMBU:
  test:
    connect_options:
      hostname: 'pyamqp://username:password@127.0.0.1:5672//'
```

# 入门案例

```yaml
├── config.yaml
├── facade.py
└── project
    ├── __init__.py
    └── service.py
```

> service.py

```python
#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from kombu import Queue
from kombu import Message
from kombu import Exchange
from logging import getLogger
from service_kombu.core.entrypoints import amqp
from service_croniter.core.entrypoints import croniter
from service_core.core.service import Service as BaseService
from service_kombu.core.dependencies.rpc import AMQPRpcProxy
from service_kombu.core.dependencies.pub import AMQPPubProducer

logger = getLogger(__name__)


class Service(BaseService):
    """ 微服务类 """

    # 微服务名称
    name = 'demo'
    # 微服务简介
    desc = 'demo'

    # 作为依赖项
    rpc = AMQPRpcProxy(alias='test')
    pub = AMQPPubProducer(alias='test')

    @amqp.rpc(alias='test')
    def test_amqp_rpc(self, body: t.Any, message: Message) -> t.Dict[t.Text, t.Any]:
        """ 测试AMQP RPC请求

        @param body: 消息内容
        @param message: 消息对象
        @return: None
        """
        logger.debug(f'yeah~ yeah~ yeah~ i got {body} with {message}')
        return {'response_from_test_amqp_rpc': body}

    @amqp.sub(alias='test', consume_options={
        'no_ack': True,
        'queues': [Queue('test', exchange=Exchange('test', 'direct', durable=True), routing_key='test')]}
    )
    def test_amqp_sub(self, body: t.Any, message: Message) -> None:
        """ 测试AMQP SUB订阅

        @param body: 消息内容
        @param message: 消息对象
        @return: None
        """
        logger.debug(f'yeah~ yeah~ yeah~ i got {body} with {message}')
        send_data = {'request_from_test_amqp_sub': body}
        reply = self.rpc.send_request(f'{self.name}.test_amqp_rpc', send_data).result
        reply_body, reply_message = reply
        logger.debug(f'yeah~ yeah~ yeah~ i got reply {reply_body} with {reply_message}')

    @croniter.cron('* * * * * */1')
    def test_amqp_pub(self) -> None:
        """ 测试AMQP PUB发布

        @return: None
        """
        send_data = {'request_from_test_amqp_pub': True}
        publish_options = {
            'routing_key': 'test',
            'serializer': 'json',
            'exchange': Exchange('test', 'direct', durable=True)
        }
        self.pub.publish(send_data, **publish_options)
```

> facade.py

```python
#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

from project import Service

service = Service()
```

# 运行服务

> core start facade --debug

# 框架集成

```python
#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import time

from kombu import Exchange
from service_kombu.constants import KOMBU_CONFIG_KEY
from service_kombu.core.standalone.amqp.pub import AMQPPubStandaloneProxy
from service_kombu.core.standalone.amqp.rpc import AMQPRpcStandaloneProxy

config = {KOMBU_CONFIG_KEY: {'connect_options': {'hostname': 'pyamqp://admin:nimda@127.0.0.1:5672//'}}}

# 其它框架集成PUB消息发布示例
with AMQPPubStandaloneProxy(config=config) as pub:
    target = 'demo.test_amqp_rpc'
    pub.publish({}, exchange=Exchange(target.split('.', 1)[0]), routing_key=target)

# 其它框架集成RPC请求调用示例
start_time = time.time()
with AMQPRpcStandaloneProxy(config=config) as rpc:
    target = 'demo.test_amqp_rpc'
    body, message = rpc.send_request(target, {}, timeout=1).result
    print(f'got response from {target}: body={body} message: {message} in {time.time() - start_time}s')
```

# 接口调试

> core shell --shell `shell`

```shell
* eventlet 0.31.1
    - platform: macOS 10.15.7
      error  : changelist must be an iterable of select.kevent objects
      issue  : https://github.com/eventlet/eventlet/issues/670#issuecomment-735488189
    - platform: macOS 10.15.7
      error  : monkey_patch causes issues with dns .local #694
      issue  : https://github.com/eventlet/eventlet/issues/694#issuecomment-806100692

2021-09-05 21:06:51,323 - 83335 - DEBUG - load subcmd service_core.cli.subcmds.debug:Debug succ
2021-09-05 21:06:51,324 - 83335 - DEBUG - load subcmd service_core.cli.subcmds.shell:Shell succ
2021-09-05 21:06:51,324 - 83335 - DEBUG - load subcmd service_core.cli.subcmds.config:Config succ
2021-09-05 21:06:51,333 - 83335 - DEBUG - load subcmd service_core.cli.subcmds.start:Start succ
2021-09-05 21:06:51,587 - 83335 - DEBUG - load subctx service_core.cli.subctxs.config:Config succ
2021-09-05 21:06:51,587 - 83335 - DEBUG - load subctx service_kombu.cli.subctx.amqp:AMQP succ
CPython - 3.9.6 (v3.9.6:db3ff76da1, Jun 28 2021, 11:49:53) [Clang 6.0 (clang-600.0.57)]
>>> with s.amqp.rpc.proxy(alias='test') as rpc:
...     rpc.send_request(f'demo.test_amqp_rpc', {}, timeout=1).result
({'response_from_test_amqp_rpc': {}}, <Message object at 0x7ff0b47dc670 with details {'state': 'RECEIVED', 'content_type': 'application/json', 'delivery_tag': 1, 'body_length': 119, 'properties': {'correlation_id': 'demo.test_amqp_rpc.abdcae97ea1e46f28930aea45b4d20ea'}, 'delivery_info': {'exchange': 'service', 'routing_key': 'service.amqp.rpc.standalone.proxy.0d71a700f7f243479a23721401cabfb4'}}>)
```

# 运行调试

> core debug --port `port`
