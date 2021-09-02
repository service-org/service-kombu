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
  - service_kombu.cli.subctxs.kombu:Kombu
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

# 接口调试

> core shell --shell `shell`

```shell
```

# 运行调试

> core debug --port `port`
