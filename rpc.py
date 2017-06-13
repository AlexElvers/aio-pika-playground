import asyncio
import json
import uuid
from functools import partial
from typing import Sequence, Any, Dict, Tuple

from aio_pika import IncomingMessage, Exchange, Queue, Message, Channel


class Handler:
    def __init__(self):
        pass


class MethodCaller:
    def __init__(self, client: "Client", method_name=None) -> None:
        self._client = client
        self._method_name = method_name

    def __getattr__(self, item: str) -> "MethodCaller":
        if self._method_name is not None:
            raise NotImplementedError("namespace RPCs are not implemented")
        return MethodCaller(self._client, item)

    def __call__(self, *args, **kwargs):
        if self._method_name is None:
            raise ValueError("cannot call 'call' directly")
        return self._client.execute_call(self._method_name, args, kwargs)


class Client:
    def __init__(self, channel: Channel, exchange: Exchange) -> None:
        self._channel = channel
        self._exchange = exchange
        self._callback_queue = None
        self._futures = {}

    async def connect(self):
        self._callback_queue = await self._channel.declare_queue(exclusive=True)
        self._callback_queue.consume(self.on_response)
        return self

    def on_response(self, message: IncomingMessage):
        future = self._futures.pop(message.correlation_id)
        future.set_result(message.body)

    @property
    def call(self):
        return MethodCaller(self)

    async def execute_call(self, method_name, args, kwargs):
        correlation_id = str(uuid.uuid4()).encode()
        future = asyncio.get_event_loop().create_future()

        self._futures[correlation_id] = future

        await self._exchange.publish(
            Message(
                encode_call(method_name, args, kwargs),
                content_type='text/plain',
                correlation_id=correlation_id,
                reply_to=self._callback_queue.name,
            ),
            routing_key='rpc_queue',
        )

        return json.loads(await future)


async def on_message(handler, exchange: Exchange, message: IncomingMessage) -> None:
    with message.process():
        method_name, args, kwargs = decode_call(message.body)

        print(" [.] call %s with args=%s and kwargs=%s" % (method_name, args, kwargs))
        if isinstance(handler, dict):
            result = handler[method_name](*args, **kwargs)
        else:
            raise ValueError

        response = json.dumps(result).encode()

        await exchange.publish(
            Message(
                body=response,
                correlation_id=message.correlation_id
            ),
            routing_key=message.reply_to
        )
        print('Request complete')


def serve(queue: Queue, handler, exchange: Exchange) -> None:
    queue.consume(partial(on_message, handler, exchange))


def method(func):
    return func


def encode_call(method_name: str, args: Sequence[Any], kwargs: Dict[str, Any]) -> bytes:
    return json.dumps(dict(method=method_name, args=args, kwargs=kwargs)).encode()


def decode_call(encoded_call: bytes) -> Tuple[str, Sequence[Any], Dict[str, Any]]:
    decoded = json.loads(encoded_call.decode())
    return decoded["method"], decoded["args"], decoded["kwargs"]
