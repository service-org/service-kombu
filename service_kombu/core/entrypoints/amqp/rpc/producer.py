#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import eventlet
import typing as t

from kombu import Consumer
from logging import getLogger
from greenlet import GreenletExit
from kombu.exceptions import ChannelError
from eventlet.greenthread import GreenThread
from kombu.exceptions import ConnectionError
from kombu.exceptions import OperationalError
from kombu.exceptions import InconsistencyError
from service_kombu.core.connect import Connection
from service_core.core.spawning import SpawningProxy
from service_core.core.decorator import AsFriendlyFunc
from service_core.core.service.entrypoint import Entrypoint
from service_core.core.service.extension import ShareExtension
from service_core.core.service.extension import StoreExtension

logger = getLogger(__name__)


class AMQPSubProducer(Entrypoint, ShareExtension, StoreExtension):
    """ AMQP消息订阅生产者类 """

    name = 'AMQPSubProducer'

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """ 初始化实例

        @param args  : 位置参数
        @param kwargs: 命名参数
        """
        self.gt_list = []
        self.stopped = False
        self.consumers = []

        Entrypoint.__init__(self, *args, **kwargs)
        ShareExtension.__init__(self, *args, **kwargs)
        StoreExtension.__init__(self, *args, **kwargs)

    def start(self) -> None:
        """ 生命周期 - 启动阶段

        @return: None
        """
        self.gt_list = [self.spawn_consumer_thread(e) for e in self.all_extensions]

    def stop(self) -> None:
        """ 生命周期 - 关闭阶段

        @return: None
        """
        self.stopped = True
        base_func = SpawningProxy(self.consumers).cancel
        wait_func = AsFriendlyFunc(base_func)
        self.consumers and wait_func()
        self.kill()

    def kill(self) -> None:
        """ 生命周期 - 强杀阶段

        @return: None
        """
        self.stopped = True
        base_func = SpawningProxy(self.gt_list).kill
        exception = (GreenletExit,)
        kill_func = AsFriendlyFunc(base_func, all_exception=exception)
        kill_func()

    def spawn_consumer_thread(self, extension: Entrypoint) -> GreenThread:
        """ 创建一个消费者协程

        @param extension: 入口对象
        @return: GreenThread
        """
        func = self.consumer
        args, kwargs, tid = (extension,), {}, f'{self}.self_consumer'
        return self.container.spawn_splits_thread(func, args, kwargs, tid=tid)

    def consumer(self, extension: Entrypoint) -> None:
        """ 消费者协程的实现

        @param extension: 入口对象
        @return: None
        """
        consumer, connection_loss = None, False
        while not self.stopped:
            try:
                if connection_loss is True:
                    logger.debug(f'{self} connection loss, start reconnecting')
                    extension.connection = Connection(**extension.connect_options)
                    logger.debug(f'{self} connection lose, reconnect success')
                    connection_loss = False
                consumer = Consumer(extension.connection, **extension.consume_options)
                consumer.consume()
                self.consumers.append(consumer)
                logger.debug(f'{self} start consuming with {extension.consume_options}')
                while True: extension.connection.drain_events()
                # 优雅处理如ctrl + c, sys.exit, kill thread时的异常
            except (KeyboardInterrupt, SystemExit, GreenletExit):
                break
                # 优雅处理ConnectionError等连接异常断开异常会去自动重试
            except (ConnectionError, ChannelError, OperationalError, InconsistencyError):
                connection_loss = True
                # 如果之前建立过连接,暂不关心当前连接状态强制关闭掉当前连接
                extension.connection and AsFriendlyFunc(extension.connection.release)()
                logger.error(f'connection error while consumer consume', exc_info=True)
                eventlet.sleep(2)
            except:
                # 应该避免其它未知异常中断当前消费者导致任务无法被及时消费
                logger.error(f'unexpected error while consumer consume', exc_info=True)
                eventlet.sleep(1)
            finally:
                consumer in self.consumers and self.consumers.remove(consumer)
