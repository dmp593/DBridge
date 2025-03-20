import os
import argparse
import asyncio
import logging
import uuid
import typing


logging.basicConfig(level=logging.INFO, format="%(message)s")


class Agent:
    token: uuid.UUID

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    def __init__(self, token: uuid.UUID, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.token = token
        self.reader = reader
        self.writer = writer


class Context:
    lock: asyncio.Lock
    agents: dict[uuid.UUID, Agent]

    def __init__(self):
        self.lock = asyncio.Lock()
        self.agents = {}

    async def is_empty(self):
        async with self.lock:
            return len(self.agents) == 0

    async def add_agent(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        token = uuid.UUID(bytes=await reader.read(16))
        logging.info(f"🎉 new agent: %s:%d (%s)", *writer.transport.get_extra_info('peername'), token)

        async with self.lock:
            self.agents[token] = Agent(token, reader, writer)


    async def pop_agent(self, token: uuid.UUID = None) -> Agent:
        while not await self.is_empty():
            if not token:
                token = list(self.agents.keys())[0]

            async with self.lock:
                agent = self.agents.pop(token)

                if not agent.reader.at_eof():
                    return agent

        return None


context = Context()


T = typing.TypeVar("T")


def parse(to: typing.Type[T], value: typing.Any, or_default: T | None = None) -> T | None:
    if isinstance(value, to):
        return value

    try:
        return to(value)
    except (ValueError, TypeError):
        return or_default


def parse_args():
    parser = argparse.ArgumentParser(description="Proxy server for forwarding connections.")

    default_host = os.getenv("HOST", "0.0.0.0")
    default_port_agents = parse(to=int, value=os.getenv("PORT_AGENTS"), or_default=7000)
    default_port_clients = parse(to=int, value=os.getenv("PORT_CLIENTS"), or_default=9000)

    parser.add_argument("-x", "--host",  default=default_host, help=f"Host to listen on (default: {default_host})")
    parser.add_argument("-a", "--port-agents", type=int, default=default_port_agents, help=f"Port for agent connections (default: {default_port_agents})")
    parser.add_argument("-c", "--port-clients", type=int, default=default_port_clients, help=f"Port for client connections (default: {default_port_clients})")

    return parser.parse_args()


async def forward(source: asyncio.StreamReader, destination: asyncio.StreamWriter):
    while True:
        data = await source.read(4096)

        if not data:
            break

        destination.write(data)
        await destination.drain()

    destination.close()
    await destination.wait_closed()


async def handle_agent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    await context.add_agent(reader, writer)


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    logging.info("🎉 new client %s:%d", *writer.transport.get_extra_info('peername'))

    if await context.is_empty():
        writer.close()
        return await writer.wait_closed()

    agent = await context.pop_agent()

    if not agent:
        writer.close()
        return await writer.wait_closed()

    agent.writer.write(b"connect")
    await agent.writer.drain()

    if await agent.reader.read(5) != b"ready":
        writer.close()
        return await writer.wait_closed()

    await asyncio.gather(
        asyncio.create_task(
            forward(reader, agent.writer)
        ),
        asyncio.create_task(
            forward(agent.reader, writer)
        )
    )


async def shutdown(loop, servers):
    logging.info(f"(𓌻‸𓌻) ᴜɢʜ...")

    while not await context.is_empty():
        agent = await context.pop_agent()
        agent.writer.close()
        await agent.writer.wait_closed()

    for server in servers:
        server.close()
        await server.wait_closed()

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    loop.call_soon(loop.stop)


async def main():
    args = parse_args()
    loop = asyncio.get_running_loop()

    servers = await asyncio.gather(
        asyncio.start_server(handle_agent, args.host, args.port_agents),
        asyncio.start_server(handle_client, args.host, args.port_clients)
    )

    try:
        logging.info("三三ᕕ( ᐛ )ᕗ")
        await asyncio.Event().wait()

    except asyncio.CancelledError:
        await shutdown(loop, servers)


if __name__ == '__main__':
    asyncio.run(main())
