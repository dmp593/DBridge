import logging

from socket import socket


def forward(src: socket, dst: socket, buffer_size: int = 4096):
    try:
        while True:
            data = src.recv(buffer_size)

            if not data:
                break

            dst.sendall(data)

    except Exception as e:
        logging.exception("Forwarding error.", exc_info=e)


def forward_and_close(src: socket, dst: socket, buffer_size: int = 4096):
    forward(src, dst, buffer_size)
    src.close()
