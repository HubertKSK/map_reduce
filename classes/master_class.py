#!/usr/bin/python
import logging.handlers
import os
import socket
import threading
from queue import Queue

# if os.path.exists("server.log"):
#     open('server.log', 'w').close()

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
FORMATTER = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
FILE_HANDLER = logging.handlers.RotatingFileHandler(
    'server.log', maxBytes=100000, backupCount=1)
FILE_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(FILE_HANDLER)


class Master(object):
    """docstring for Master."""

    def __init__(self, inputfile):
        super(Master, self).__init__()
        self.HEADER = 64
        self.PORT = 5050
        self.SERVER = socket.gethostbyname(socket.gethostname())
        self.ADDR = (self.SERVER, self.PORT)
        self.FORMAT = 'utf-8'
        self.DISCONNECT_MESSAGE = "!DISCONNECT"

        self.inputfile = inputfile
        LOGGER.info("Starting Master...")
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(self.ADDR)

        self.start()

    def handle_client(self, conn, addr, client_uuid, queue):
        LOGGER.info(f"[NEW CONNECTION] {addr} {client_uuid} connected.")

        connected = True
        while connected:
            # msg_length = conn.recv(self.HEADER).decode(self.FORMAT)
            # if msg_length:
            #     msg_length = int(msg_length)
            #     msg = conn.recv(msg_length).decode(self.FORMAT)
            #     LOGGER.info(f"[{addr} {client_uuid}] {msg}")
            #     conn.send("ACK".encode(self.FORMAT))
            #
            #     if msg == self.DISCONNECT_MESSAGE:
            #         LOGGER.info(
            #             f"[DISCONECTED] {addr} {client_uuid} disconected")
            #         connected = False

            try:
                data = queue.get(timeout=30.0)
                conn.send(data.encode(self.FORMAT))
            except Exception as e:
                LOGGER.debug(f"Queue empty")


        conn.close()

    def await_start(self, queue):
        print("Console started.")
        LOGGER.info(f"Console Started")
        while True:
            arg = input("> ")
            if arg == "start":
                LOGGER.info(f"Activated start")
                print("start")
                queue.put("hello")
                queue.put("Server")

            else:
                print("Bad arg")

    def start(self):
        self.server.listen()
        LOGGER.info(f"[LISTENING] Server is listening on {self.SERVER}")
        q = Queue()
        console = threading.Thread(
            target=self.await_start, args=(q,))
        console.start()
        while True:
            conn, addr = self.server.accept()
            client_UUID = conn.recv(36).decode(self.FORMAT)
            thread = threading.Thread(
                target=self.handle_client, args=(conn, addr, client_UUID, q))
            thread.start()
            LOGGER.info(f"[ACTIVE CONNECTIONS] {threading.active_count() - 2}")


if __name__ == "__main__":
    inputfile = "'/Users/hubertkowalczyk/Documents/Studia/sem3/map_reduce/testcode.py'"
    master_instance = Master(inputfile)
