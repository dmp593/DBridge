import socket
import logging
import os
import threading
from threading import Thread, Lock
from time import sleep

from forward import bidirectional


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


threads = []
lock = Lock()


PROXY_HOST = os.getenv("PROXY_HOST", "localhost")
PROXY_PORT = int(os.getenv("PROXY_PORT", 3333))

DATABASE_HOST = os.getenv("DATABASE_HOST", "localhost")
DATABASE_PORT = int(os.getenv("DATABASE_PORT", 3306))


def connect(host, port) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    logging.info(f"Connected to server at {host}:{port}")

    return sock


def start_forwarding(parent_thread=None):
    proxy_server: socket.socket = connect(PROXY_HOST, PROXY_PORT)
    db_server = connect(DATABASE_HOST, DATABASE_PORT)
    bidirectional(proxy_server, db_server)


def main():
    start_forwarding()


if __name__ == "__main__":
    main()