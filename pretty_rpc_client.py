import asyncio

from aio_pika import connect

import rpc


async def main(loop):
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost/", loop=loop)

    # Creating a channel
    channel = await connection.channel()

    client = await rpc.Client(channel, channel.default_exchange).connect()

    print(" [x] Requesting fib(30)")
    # result = await client.execute_call("fib", [30], {})
    result = await client.call.fib(30)
    print(" [.] Got %r" % result)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
