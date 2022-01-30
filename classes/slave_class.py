#!/usr/bin/python

import logging.handlers
import os
import socket
import uuid
import json
from nltk.tokenize import word_tokenize

if not os.path.exists("log"):
    os.makedirs("log")

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
FORMATTER = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
FILE_HANDLER = logging.handlers.RotatingFileHandler(
    'log/client.log', maxBytes=100000, backupCount=0)
FILE_HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(FILE_HANDLER)


class Slave(object):
    """docstring for Slave."""

    def __init__(self):
        super(Slave, self).__init__()
        self.msg = None
        self.HEADER = 64
        self.PORT = 5050
        self.SERVER = "127.0.0.1"
        self.ADDR = (self.SERVER, self.PORT)
        self.FORMAT = 'utf-8'
        self.DISCONNECT_MESSAGE = "!DISCONNECT"
        self.UUID = uuid.uuid4()

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

    def map(self, contents):
        results = []

        contents = ' '.join(contents)

        data = word_tokenize(contents.lower())
        for word in data:
            results.append((word, 1))

        return results

    def dissconect(self):
        self.send(self.DISCONNECT_MESSAGE)

    def reduce(self, key, values):
        return key, sum(value for value in values)

    def run_reduce(self, shuffle):

        key_values_map = dict(shuffle)

        key_value_list = []
        for key in key_values_map:
            key_value_list.append(self.reduce(key, key_values_map[key]))

        return key_value_list

    def listen(self):
        LOGGER.debug(f"[{self.UUID}] listening")
        print("Listening")
        msg_length = self.client.recv(self.HEADER).decode(self.FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            self.msg = self.client.recv(msg_length).decode(self.FORMAT)
            LOGGER.debug(f"[{self.UUID}] Received Message ")
            self.client.send("ACK".encode(self.FORMAT))
            self.controller()

    def controller(self):
        control_state, msg = json.loads(self.msg)
        result = "null"
        if control_state == 0:
            LOGGER.info(f"[{self.UUID}] Starting map")
            result = self.map(msg)
            self.send(json.dumps(result))

        elif control_state == 1:
            LOGGER.info(f"[{self.UUID}] Starting reduce")
            result = self.run_reduce(msg)
            self.send(json.dumps(result))

        else:
            LOGGER.error(f"[{self.UUID}] Unknown command")


if __name__ == "__main__":
    print("Test run")
    inputfile = "'/Users/hubertkowalczyk/Documents/Studia/sem3/map_reduce/testcode.py'"
    slave_instance = Slave()
    slave_instance.send("hello")
    slave_instance.send("World")
    slave_instance.listen()
    slave_instance.dissconect()
