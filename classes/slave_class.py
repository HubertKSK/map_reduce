#!/usr/bin/python

import subprocess
import socket


class Slave(object):
    """docstring for Slave."""

    def __init__(self, inputfile):
        super(Slave, self).__init__()
        self.HEADER = 64
        self.PORT = 5050
        self.SERVER = "127.0.0.1"
        self.ADDR = (self.SERVER, self.PORT)
        self.FORMAT = 'utf-8'
        self.DISCONNECT_MESSAGE = "!DISCONNECT"

        self.inputfile = inputfile
        print("starting Slave")

        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect(self.ADDR)

    def send(self, msg):
        message = msg.encode(self.FORMAT)
        msg_length = len(message)
        send_length = str(msg_length).encode(self.FORMAT)
        send_length += b' ' * (self.HEADER - len(send_length))
        self.client.send(send_length)
        self.client.send(message)
        print(self.client.recv(2048).decode(self.FORMAT))

    def dissconect(self):
        self.send(self.DISCONNECT_MESSAGE)

    def wait(self):
        pass

    def run(self):
        p = subprocess.Popen(f"python3 {self.inputfile}", shell=True)
        out, err = p.communicate()
        print(out)

if __name__ == "__main__":
    inputfile = "'/Users/hubertkowalczyk/Documents/Studia/sem3/map_reduce/testcode.py'"
    slave_instance = Slave(inputfile)
    slave_instance.send("hello")
    slave_instance.send("World")
    slave_instance.dissconect()

