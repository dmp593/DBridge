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
        logging.info(f"🎉 new agent (%s) %s:%d", token, *writer.transport.get_extra_info('peername'))

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


def parse_bool(value: typing.Any, or_default: typing.Any | None = None) -> typing.Any | None:
    if isinstance(value, int):
        return value != 0

    if isinstance(value, str):
        match value.lower().strip():
            case "yes" | "y" | "t" | "true" | "s" | "sim" | "yea" | "yeah" | "on" | 1 | "1":
                return True
            case "no" | "n" | "f" | "false" | "ñ" | "não" | "nao" | "nop" | "nope" | "off" | 0 | "0" | "":
                return False
            case _:
                return or_default


def parse_args():
    parser = argparse.ArgumentParser(description="Proxy server for forwarding connections.")

    default_host = os.getenv("HOST", "0.0.0.0")
    default_port_liveness = parse(to=int, value=os.getenv("PORT_LIVENESS"), or_default=3130)
    default_port_readiness = parse(to=int, value=os.getenv("PORT_READINESS"), or_default=4260)

    default_port_agents = parse(to=int, value=os.getenv("PORT_AGENTS"), or_default=7000)
    default_port_clients = parse(to=int, value=os.getenv("PORT_CLIENTS"), or_default=9000)

    default_wait_agent_max_tries = parse(to=int, value=os.getenv("WAIT_AGENT_MAX_TRIES"), or_default=10)
    default_wait_agent_retry_sleep_time = parse(to=float, value=os.getenv("WAIT_AGENT_SLEEP_TIME"), or_default=0.7)

    default_use_ssl = parse_bool(value=os.getenv("USE_SSL"), or_default=False)
    default_ssl_cert = os.getenv("SSL_CERT", None)
    default_ssl_key = os.getenv("SSL_KEY", None)

    parser.add_argument("-x", "--host", default=default_host,
                        help=f"Host to listen on (default: {default_host})")

    parser.add_argument("-l", "--port-liveness", type=int, default=default_port_liveness,
                        help=f"Port for liveness prob (default: {default_port_liveness})")

    parser.add_argument("-r", "--port-readiness", type=int, default=default_port_readiness,
                        help=f"Port for readiness prob (default: {default_port_readiness})")

    parser.add_argument("-a", "--port-agents", type=int, default=default_port_agents,
                        help=f"Port for agent connections (default: {default_port_agents})")

    parser.add_argument("-p", "--port-clients", type=int, default=default_port_clients,
                        help=f"Port for client connections (default: {default_port_clients})")

    parser.add_argument("-t", "--wait-agent-max-tries", type=int, default=default_wait_agent_max_tries,
                        help=f"Maximum number of attempts to find an agent (default: {default_wait_agent_max_tries})")

    parser.add_argument("-z", "--wait-agent-sleep-time", type=float, default=default_wait_agent_retry_sleep_time,
                        help=f"Time to sleep between retries when no agent is available (default: {default_wait_agent_retry_sleep_time})")

    parser.add_argument("-s", "--ssl", action="store_true", default=default_use_ssl,
                        help="Enable SSL for secure connections (default: no)")

    parser.add_argument("-c", "--cert", default=default_ssl_cert,
                        help=f"SSL certificate file (optional)")

    parser.add_argument("-k", "--key", default=default_ssl_key,
                        help=f"SSL private key file (optional)")

    return parser.parse_args()

async def forward(agent_token: uuid.UUID, source_peername: tuple[str, int], source: asyncio.StreamReader, destination: asyncio.StreamWriter):
    try:
        while True:
            data = await source.read(4096)

            if not data:
                break

            destination.write(data)
            await destination.drain()
    except asyncio.IncompleteReadError:
        logging.info(
            "😢 { (%s) source: %s:%d <-> destination: %s:%d } Unexpected EOF",
            agent_token, *source_peername, *destination.transport.get_extra_info('peername')
        )

    except ConnectionResetError:
        logging.info(
            "😩 { (%s) source: %s:%d <-> destination: %s:%d } Connection reset",
            agent_token, *source_peername, *destination.transport.get_extra_info('peername')
        )

    except Exception:
        logging.info(
            "👽 { (%s) source: %s:%d <-> destination: %s:%d } Something crazy happened",
            agent_token, *source_peername, *destination.transport.get_extra_info('peername')
        )

    finally:
        destination.close()
        await destination.wait_closed()


async def handle_liveness(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    writer.write("🚀 Alive and forwarding packets like a champ!".encode())
    await writer.drain()

    writer.close()
    await writer.wait_closed()


async def handle_readiness(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    if await context.is_empty():
        message = "⚠️ Not ready! Waiting for agents to join..."
    else:
        message = "🚀 Ready to forward packets like a pro!"

    writer.write(message.encode("utf-8"))
    await writer.drain()

    writer.close()
    await writer.wait_closed()


async def handle_agent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    await context.add_agent(reader, writer)


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, wait_agent_max_tries: int, wait_agent_sleep_time: float):
    client_peername = writer.transport.get_extra_info('peername')
    logging.info("🎉 new client %s:%d", *client_peername)

    agent = None

    for i in range(wait_agent_max_tries):  # max tries to wait for an agent
        agent = await context.pop_agent()
        if agent: break

        logging.info("🍃 no agents available, retrying in %.2f sec(s)...", wait_agent_sleep_time)
        await asyncio.sleep(0.7)  # sleep <wait_agent_sleep_time> seconds, waiting for an agent...
    else:
        logging.info("👻 no agents available.")

    if not agent:
        writer.close()
        return await writer.wait_closed()

    agent.writer.write(b"connect")
    await agent.writer.drain()

    agent_peername = agent.writer.transport.get_extra_info('peername')

    if await agent.reader.read(5) != b"ready":
        writer.close()
        await writer.wait_closed()

        agent.writer.close()
        await agent.writer.wait_closed()

        return

    try:
        await asyncio.gather(
            asyncio.create_task(
                forward(agent.token, client_peername, reader, agent.writer)
            ),
            asyncio.create_task(
                forward(agent.token, agent_peername, agent.reader, writer)
            )
        )
    except Exception:
        logging.info(
            "🛸 { agent %s:%d (%s) <-> client %s:%d } Something crazy happened.",
            *agent_peername, agent.token, client_peername
        )

        agent.writer.close()
        await agent.writer.wait_closed()

        writer.close()
        await writer.wait_closed()


async def shutdown(loop, servers: list):
    logging.info("️( -_•)︻デ═一💥 killing...")

    await asyncio.sleep(0.5)  # Allow logs to flush

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
            handle_liveness, "localhost", args.port_liveness
        ),

        asyncio.start_server(
            handle_readiness, "localhost", args.port_readiness
        ),

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
        logging.info("🛑 Proxy was cancelled.")
        await shutdown(loop, servers)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🔫 Interrupted by user.")