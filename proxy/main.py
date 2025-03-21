import os
import argparse
import asyncio
import logging
import uuid
import typing
import ssl


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
    agents: set[Agent]

    def __init__(self):
        self.lock = asyncio.Lock()
        self.agents = set()

    async def is_empty(self):
        async with self.lock:
            return len(self.agents) == 0

    async def add_agent(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        token = uuid.UUID(bytes=await reader.read(16))
        logging.info(f"🎉 new agent (%s): %s:%d", token, *writer.transport.get_extra_info('peername'))

        agent = Agent(token, reader, writer)

        async with self.lock:
            self.agents.add(agent)

    async def pop_agent(self) -> Agent | None:
        while True:
            try:
                async with self.lock:
                    agent = self.agents.pop()

                if agent.reader.at_eof():
                    logging.info("🗑️ agent (%s) discarded", agent.token)
                    agent.writer.close()
                    await agent.writer.wait_closed()
                    continue

                logging.info("🍾 agent (%s) popped", agent.token)
                return agent

            except KeyError:
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

    default_wait_agent_max_tries = parse(to=int, value=os.getenv("MAX_TRIES"), or_default=10)
    default_wait_agent_retry_sleep_time = parse(to=float, value=os.getenv("SLEEP_TIME"), or_default=0.7)

    parser.add_argument("-x", "--host", default=default_host,
                        help=f"Host to listen on (default: {default_host})")

    parser.add_argument("-a", "--port-agents", type=int, default=default_port_agents,
                        help=f"Port for agent connections (default: {default_port_agents})")

    parser.add_argument("-p", "--port-clients", type=int, default=default_port_clients,
                        help=f"Port for client connections (default: {default_port_clients})")

    parser.add_argument("-t", "--wait-agent-max-tries", type=int, default=default_wait_agent_max_tries,
                        help=f"Maximum number of attempts to find an agent (default: {default_wait_agent_max_tries})")

    parser.add_argument("-z", "--wait-agent-sleep-time", type=float, default=default_wait_agent_retry_sleep_time,
                        help=f"Time to sleep between retries when no agent is available (default: {default_wait_agent_retry_sleep_time})")

    parser.add_argument("-s", "--ssl", action="store_true",
                        help="Enable SSL for secure connections")

    parser.add_argument("-c", "--cert", default="cert.pem",
                        help="SSL certificate file (default: cert.pem)")

    parser.add_argument("-k", "--key", default="key.pem",
                        help="SSL private key file (default: key.pem)")

    return parser.parse_args()

async def forward(source: asyncio.StreamReader, destination: asyncio.StreamWriter):
    try:
        while True:
            data = await source.read(4096)
            if not data:
                break
            destination.write(data)
            await destination.drain()

    except asyncio.IncompleteReadError:
        logging.info("😢 unexpected EOF")

    except ConnectionResetError:
        logging.info("😩 connection reset")

    finally:
        destination.close()
        await destination.wait_closed()


async def handle_agent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    await context.add_agent(reader, writer)


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, wait_agent_max_tries: int, wait_agent_sleep_time: float):
    logging.info("🎉 new client %s:%d", *writer.transport.get_extra_info('peername'))

    agent = None

    for i in range(wait_agent_max_tries):  # max tries to wait for an agent
        agent = await context.pop_agent()
        if agent: break

        logging.info("🍃 no agents available, retrying in %.2f second(s)...", wait_agent_sleep_time)
        await asyncio.sleep(0.7)  # sleep <wait_agent_sleep_time> seconds, waiting for an agent...
    else:
        logging.info("👻 no agents available.")

    if not agent:
        writer.close()
        return await writer.wait_closed()

    agent.writer.write(b"connect")
    await agent.writer.drain()

    if await agent.reader.read(5) != b"ready":
        writer.close()
        await writer.wait_closed()

        agent.writer.close()
        await agent.writer.wait_closed()

        return

    await asyncio.gather(
        asyncio.create_task(
            forward(reader, agent.writer)
        ),
        asyncio.create_task(
            forward(agent.reader, writer)
        )
    )


async def shutdown(loop, servers):
    logging.info("️( -_•)︻デ═一💥 killing...")

    while not await context.is_empty():
        agent = await context.pop_agent()

        if agent:
            agent.writer.close()
            await agent.writer.wait_closed()

    for server in servers:
        server.close()
        await server.wait_closed()

    tasks = {t for t in asyncio.all_tasks() if t is not asyncio.current_task()}

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    loop.call_soon(loop.stop)


async def main():
    args = parse_args()
    loop = asyncio.get_running_loop()

    ssl_context = None
    if args.ssl:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(certfile=args.cert, keyfile=args.key)

    servers = await asyncio.gather(
        asyncio.start_server(
            handle_agent, args.host, args.port_agents, ssl=ssl_context
        ),

        asyncio.start_server(
            lambda r, w: handle_client(r, w, args.wait_agent_max_tries, args.wait_agent_sleep_time),
            args.host,
            args.port_clients,
            ssl=ssl_context
        )
    )

    try:
        logging.info("三三ᕕ( ᐛ )ᕗ")
        await asyncio.Event().wait()

    except asyncio.CancelledError:
        await shutdown(loop, servers)


if __name__ == '__main__':
    asyncio.run(main())
