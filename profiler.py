import asyncio
import logging as log
import time
from threading import Thread
from abc import ABC
from datetime import datetime
from contextlib import contextmanager
from functools import wraps
from collections import deque, namedtuple


class AbstractProfilerClient(ABC):

    @staticmethod
    def _undescorize(value):
        return str(value).replace(" ", "_").replace("=", "_")

    def serialize_event(self, event):
        tags = ','.join(['{k}={v}'.format(k=self._undescorize(k), v=self._undescorize(v))
                         for k,v in event.tags.items()])

        return '{timeseries},{tags} value={value} {timestamp:.0f}'.format(
            timeseries=self._undescorize(event.timeseries),
            tags=tags, value=event.value, timestamp=event.timestamp)

    def serialize_events(self, events):
        data = "\n".join([self.serialize_event(e) for e in events])
        return data

    def get_headers(self):
        return {'Content-Type': 'application/octet-stream'}


    async def send_events(self, events):
        raise NotImplemented("You should override this method in specific client class")


class BaseDummyProfilerClient(AbstractProfilerClient):

    def __init__(self, logger=None):
        self._logger = logger or log

    def _send_events(self, events):
        data = self.serialize_events(events)
        self._logger.debug("{delimiter} Sending profiling data... {delimiter}".format(delimiter="#" * 27))
        self._logger.debug(data)
        self._logger.debug("#" * 80)


class AsyncDummyProfilerClient(BaseDummyProfilerClient):
    async def send_events(self, events):
        self._send_events(events)


class BlockingDummyProfilerClient(BaseDummyProfilerClient):
    def send_events(self, events):
        self._send_events(events)


class RequestsProfilerClient(AbstractProfilerClient):

    def __init__(self, url, timeout, **kwargs):
        try:
            from requests import post
        except ImportError as e:
            raise ("Cannot import required requests library: {}".format(e))
        else:
            self._requests_post = post
            self._url = url
            self._timeout = timeout
            self._post_kwargs = kwargs

    def send_events(self, events):
        data = self.serialize_events(events)
        resp = self._requests_post(
            url=self._url,
            data=data.encode(),
            headers=self.get_headers(),
            timeout=self._timeout,
            **self._post_kwargs
        )
        resp.raise_for_status()


class AiohttpProfilerClient(AbstractProfilerClient):

    def __init__(self, url, timeout, **kwargs):
        try:
            from aiohttp import ClientSession
        except ImportError as e:
            raise ("Cannot import required ClientSession client: {}".format(e))
        else:
            self._session_class = ClientSession
            self._url = url
            self._timeout = timeout
            self._session_kwargs = kwargs

    async def send_events(self, events):
        data = self.serialize_events(events)
        async with self._session_class(**self._session_kwargs) as session:
            async with session.post(url=self._url, data=data.encode(), headers=self.get_headers(),
                                    timeout=self._timeout) as resp:
                resp.raise_for_status()


class TornadoProfilerClient(AbstractProfilerClient):

    def __init__(self, url, timeout, **kwargs):
        try:
            from tornado.platform.asyncio import to_asyncio_future
            from tornado.httpclient import AsyncHTTPClient
        except ImportError as e:
            raise ("Cannot import required Tornado client: {}".format(e))
        else:
            self._client_class = AsyncHTTPClient
            self._future_converter = to_asyncio_future
            self._url = url
            self._timeout = timeout
            self._client_kwargs = kwargs

    async def send_events(self, events):
        data = self.serialize_events(events)
        client = self._client_class()
        try:
            response = await self._future_converter(self._client_class().fetch(self._url, method='POST', headers=self.get_headers(),
                                                        body=data.encode(), request_timeout=self._timeout,
                                                        **self._client_kwargs))
        finally:
            client.close()
        response.rethrow()


Event = namedtuple('Event', "timeseries tags value timestamp")


class Profiler:
    """
    Профилировщик функций и участков кода.

    Поддерживаются, как асинхронные сервисы c AbstractEventLoop-совместимым loop'ом, так и синхронные.
    Отправка событий пачками через AbstractProfilerClient-совместимый клиент.

    В основной нити исполнения - инициализация

    >>>from profiler import create_and_run_profiler, Profiler, AsyncDummyProfilerClient, BlockingDummyProfilerClient

    После инициализации loop'а
    >>>profiler = create_and_run_profiler(AsyncDummyProfilerClient(), flush_interval=100)
    или для синхронного сервиса
    >>>profiler = create_and_run_profiler(BlockingDummyProfilerClient(), flush_interval=100, threaded=True)

    Использование:

    >>>from profiler import Profiler

    Синглтон, поэтому можно инициализировать многократно
    >>>profiler = Profiler()

    >>>@profiler.profile_function(timeseries="service_name")
    >>>async def some_function(*args, **kwargs):
            ...
    >>>     with profiler.profile_code("critical section", success_tags={"api version": 1},
    >>>                                error_tags={"status": "custom error status"}) as p:
                ...
            ...
    Завершение работы:
    >>>profiler.shutdown()

    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance


    @classmethod
    def setup(cls, storage_client, default_timeseries='profiling',
              flush_interval=None, queue_size=None, loop=None, logger=None, threaded=False):
        cls._storage_client = storage_client                # Клиент для отправки с базовым классом AbstractClient
        cls._loop = loop or asyncio.get_event_loop()
        cls._flush_interval = flush_interval or 60          # Интервал отправки, в секундах
        cls._events = deque(maxlen=queue_size or 1000)      # Thread safe
        cls._default_timeseries = default_timeseries
        cls._logger = logger or log                         # Логгер, по умолчанию из стандартной библиотеки
        cls._threaded = threaded                            # Профилирование в отдельной нити исполнения для неасинхронных сервисов
        cls._closing = False


    def run(self):

        if self._threaded:

            def send_stat():
                self._logger.info("~~~~~~~~~~~ Profiler started in a separate thread... ~~~~~~~~~~~")
                while True:
                    if not self._closing:
                        time.sleep(self._flush_interval)
                    if self._events:
                        events_to_flush = [self._events.popleft() for _ in range(len(self._events))]
                        try:
                            self._storage_client.send_events(events_to_flush)
                        except Exception as exc:
                            self._handle_sending_error(exc, events_to_flush)
                    if self._closing:
                        self._logger.info("~~~~~~~~~~~ Profiler shutting down... ~~~~~~~~~~~")
                        break

            t = Thread(target=send_stat)
            t.start()

        else:
            if not self._loop:
                raise RuntimeError("Cannot get event loop")

            async def send_stat():
                self._logger.info("~~~~~~~~~~~ Profiler started async... ~~~~~~~~~~~")
                while True:
                    if not self._closing:
                        await asyncio.sleep(self._flush_interval)
                    if self._events:
                        events_to_flush = [self._events.popleft() for _ in range(len(self._events))]
                        try:
                            await self._storage_client.send_events(events_to_flush)
                        except Exception as exc:
                            self._handle_sending_error(exc, events_to_flush)
                    if self._closing:
                        self._logger.info("~~~~~~~~~~~ Profiler shutting down... ~~~~~~~~~~~")
                        break

            self._loop.create_task(send_stat())


    def shutdown(self):
        self._closing = True

    def _handle_sending_error(self, exception, events_to_flush):
        events_to_flush.reverse()
        self._events.extendleft(events_to_flush)
        self._logger.error('Error while sending profiling stat: {}'.format(str(exception)))

    def _register_event(self, timeseries, start_time, tags):
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds() * 1000   # миллисекунды
        ns_timestamp = end_time.timestamp() * 10 ** 9               # В наносекундах
        try:
            self._events.append(Event(timeseries=timeseries or self._default_timeseries, tags=tags,
                                      value=duration, timestamp=ns_timestamp))
        except AttributeError:
            # При использовании декорируемых профилировщиком функций в юнит-тестах,
            # когда профилировщик не запущен, игноририруем это исключение
            log.info("Profiler is not setup")

    def _register_success_event(self, timeseries, start_time, base_tags, success_tags):
        success_tags = success_tags or {}
        tags = {**base_tags, **{'status': 'success'}, **success_tags}
        self._register_event(timeseries, start_time, tags)

    def _register_error_event(self, timeseries, start_time, base_tags, error_tags):
        error_tags = error_tags or {}
        tags = {**base_tags, **{'status': 'error'}, **error_tags}
        self._register_event(timeseries, start_time, tags)

    # Декоратор для функций
    def profile_function(self, marker=None, success_tags=None, error_tags=None, timeseries=None):
        """
        Регистрирует событие с маркером и тэгами для успешного и с ошибкой сценариев для декорируемой функции.

        marker - маркер - по умолчанию, имя функции
        success_tags - в случае успешного завершения, есть по умолчанию {'status': 'success'} - может быть перезаписан
        error_tags - в случае завершения с ошибкой, есть по умолчанию {'status': 'error'} - может быть перезаписан
        """

        def decorator(f):
            base_tags = {'marker': marker or f.__name__}

            if asyncio.iscoroutinefunction(f):
                @wraps(f)
                async def decorated_function(*args, **kwargs):

                    start_time = datetime.utcnow()
                    try:
                        result = await f(*args, **kwargs)
                    except Exception:
                        self._register_error_event(timeseries, start_time, base_tags, error_tags)
                        raise
                    else:
                        self._register_success_event(timeseries, start_time, base_tags, success_tags)
                        return result
                return decorated_function
            else:
                @wraps(f)
                def decorated_function(*args, **kwargs):
                    start_time = datetime.utcnow()
                    try:
                        result = f(*args, **kwargs)
                    except Exception:
                        self._register_error_event(timeseries, start_time, base_tags, error_tags)
                        raise
                    else:
                        self._register_success_event(timeseries, start_time, base_tags, success_tags)
                        return result
                return decorated_function

        return decorator

    # Контекстный менеджер для участков кода
    @contextmanager
    def profile_code(self, marker, success_tags=None, error_tags=None, timeseries=None):
        """
        Регистрирует событие с маркером и тэгами для успешного и с ошибкой сценариев для участка кода внутри данного контекста.

        marker - маркер - маркер для участка кода
        success_tags - в случае успешного завершения, есть по умолчанию {'status': 'success'} - может быть перезаписан
        error_tags - в случае завершения с ошибкой, есть по умолчанию {'status': 'error'} - может быть перезаписан
        """

        start_time = datetime.utcnow()
        base_tags = {'marker': marker}

        try:
            yield
        except Exception:
            self._register_error_event(timeseries, start_time, base_tags, error_tags)
            raise
        else:
            self._register_success_event(timeseries, start_time, base_tags, success_tags)


def create_and_run_profiler(client, **kwargs):
    Profiler.setup(client, **kwargs)
    p = Profiler()
    p.run()
    return p
