import os
import asyncio
import logging
import uuid


logging.basicConfig(level=logging.INFO, format="%(message)s")


HOST = os.getenv("HOST", "0.0.0.0")
PORT_AGENTS =  int(os.getenv("PORT_AGENTS", 4000))
PORT_CLIENTS = int(os.getenv("PORT_CLIENTS", 3000))


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
    loop = asyncio.get_running_loop()

    servers = await asyncio.gather(
        asyncio.start_server(handle_agent, HOST, PORT_AGENTS),
        asyncio.start_server(handle_client, HOST, PORT_CLIENTS)
    )

    try:
        logging.info("三三ᕕ( ᐛ )ᕗ")
        await asyncio.Event().wait()

    except asyncio.CancelledError:
        await shutdown(loop, servers)


if __name__ == '__main__':
    asyncio.run(main())
