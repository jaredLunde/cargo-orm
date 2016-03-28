import signal
import asyncio
from cargo.aio import aiodb


def get_result(result, *args, **kwargs):
    print(result.result())


def select(x):
    async def _select():
        return (x, await aiodb.clear_copy().select(1, 2, 3, 4, 5))
    return _select()

async def get_range(loop):
    futures = []
    for x in range(1000):
        futures.append(select(x))
    results = []
    for i, x in enumerate(await asyncio.gather(*futures, loop=loop)):
        results.append((i, x))
    return results


async def get_range_as_completed(loop):
    futures = []
    for x in range(5000):
        futures.append(select(x))
    results = []
    for i, x in enumerate(asyncio.as_completed(futures, loop=loop)):
        results.append((i, await x))
    return results


async def main(loop):
    results = await get_range_as_completed(loop)
    return results


def handler(future, loop):
    future.cancel()
    loop.stop()


def close_aiodb(loop):
    aiodb.close()
    loop.run_until_complete(aiodb.wait_closed())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(aiodb.open(loop=loop))
    future = asyncio.ensure_future(main(loop), loop=loop)
    loop.add_signal_handler(signal.SIGINT, handler, future, loop)
    print(loop.run_until_complete(future))
    close_aiodb(loop)
    loop.stop()
