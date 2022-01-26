#!/usr/bin/python

import subprocess
import socket
import logging
import logging.handlers
import os
import uuid

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
FORMATTER = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
FILE_HANDLER = logging.handlers.RotatingFileHandler(
    'client.log', maxBytes=100000, backupCount=1)
FILE_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(FILE_HANDLER)

class Slave(object):
    """docstring for Slave."""

    def __init__(self):
        super(Slave, self).__init__()
        self.HEADER = 64
        self.PORT = 5050
        self.SERVER = "127.0.0.1"
        self.ADDR = (self.SERVER, self.PORT)
        self.FORMAT = 'utf-8'
        self.DISCONNECT_MESSAGE = "!DISCONNECT"
        self.UUID = uuid.uuid4()
        LOGGER.info(self.UUID)

        LOGGER.info(f"[{self.UUID}] starting Slave")

        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect(self.ADDR)
        self.register()

    def register(self):
        message = str(self.UUID)
        message = message.encode(self.FORMAT)
        self.client.send(message)

    def send(self, msg):
        LOGGER.debug(f"[{self.UUID}] Sending msg: {msg}")
        message = msg.encode(self.FORMAT)
        msg_length = len(message)
        send_length = str(msg_length).encode(self.FORMAT)
        send_length += b' ' * (self.HEADER - len(send_length))
        self.client.send(send_length)
        self.client.send(message)
        LOGGER.debug(self.client.recv(2048).decode(self.FORMAT))

    def dissconect(self):
        self.send(self.DISCONNECT_MESSAGE)

    def listen(self):
        LOGGER.debug(f"[{self.UUID}] listening")
        print("Listening")
        self.msg = self.client.recv(2048).decode(self.FORMAT)
        LOGGER.debug(f"[{self.UUID}] Received Message \"{self.msg}\"")
        print(self.msg)

if __name__ == "__main__":
    print("Test run")
    inputfile = "'/Users/hubertkowalczyk/Documents/Studia/sem3/map_reduce/testcode.py'"
    slave_instance = Slave()
    slave_instance.send("hello")
    slave_instance.send("World")
    slave_instance.listen()
    slave_instance.dissconect()

