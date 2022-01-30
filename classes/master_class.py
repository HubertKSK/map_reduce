#!/usr/bin/python
import logging.handlers
import os
import socket
import threading
from queue import Queue
import json

# if os.path.exists("server.log"):
#     open('server.log', 'w').close()

if not os.path.exists("log"):
    os.makedirs("log")

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
FORMATTER = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
FILE_HANDLER = logging.handlers.RotatingFileHandler('log/server.log', maxBytes=100000, backupCount=0)
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
        self.clients = 0
        self.send_queue = Queue()
        self.recive_queue = Queue()

        self.inputfile = inputfile
        LOGGER.info("Starting Master...")
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(self.ADDR)

        self.start()

    def handle_client(self, conn, addr, client_uuid):
        def send(msg):
            LOGGER.debug(f"[{addr} {client_uuid}] Sending msg: {msg}")
            message = msg.encode(self.FORMAT)
            msg_length = len(message)
            send_length = str(msg_length).encode(self.FORMAT)
            send_length += b' ' * (self.HEADER - len(send_length))
            conn.send(send_length)
            conn.send(message)
            LOGGER.debug(conn.recv(2048).decode(self.FORMAT))

        def listen():
            msg = "NULL"
            LOGGER.debug(f"[{addr} {client_uuid}] listening")
            msg_length = conn.recv(self.HEADER).decode(self.FORMAT)
            if msg_length:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(self.FORMAT)
                LOGGER.debug(f"[{addr} {client_uuid}] Received Message ")
                conn.send("ACK".encode(self.FORMAT))

            return msg

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
                data = self.send_queue.get(timeout=30.0)
                print(f"[{addr} {client_uuid}] Trying to send Data...")
                LOGGER.debug(f"[{addr} {client_uuid}] Trying to send Data...")
                send(json.dumps(data))
                # conn.send(jsonStr.encode(self.FORMAT))
                print(f"[{addr} {client_uuid}] Data Send")
                LOGGER.debug(f"[{addr} {client_uuid}] Data Send")
                msg = json.loads(listen())
                # msg_length = conn.recv(self.HEADER).decode(self.FORMAT)
                # if msg_length:
                #     msg_length = int(msg_length)
                #     msg = conn.recv(msg_length).decode(self.FORMAT)
                #     LOGGER.debug(f"[{addr} {client_uuid}] {msg}")
                #     print(f"[{addr} {client_uuid}] {msg}")
                #     conn.send("ACK".encode(self.FORMAT))
                self.recive_queue.put(msg)


            except Exception as e:
                LOGGER.debug(f"Queue empty")

        conn.close()

    def split_file_by_lines(self, file_path, clients_quantity):
        file_number = 0

        with open(file_path, "r") as file:
            lines_quantity = len(file.readlines())
            file.seek(0)
            lines_quantity_to_split = int(lines_quantity / clients_quantity) + 1

            list_of_chunks = []
            new_file = []
            lines = file.readlines()
            for i in range(0, len(lines), lines_quantity_to_split):
                list_of_chunks.append(lines[i:i + lines_quantity_to_split])

            for chunk in list_of_chunks:
                split = []
                for line in chunk:
                    split.append(line)
                new_file.append(split)
                file_number += 1

        return new_file

    def run_shuffle(self, map_file):
        # TODO: FIX
        dictionary = {}
        for idx, val in enumerate(map_file):
            mapper_results = val
            for (key, value) in mapper_results:
                if not (key in dictionary):
                    dictionary[key] = []
                    dictionary[key].append(value)
                dictionary[key].append(value)

        number_to_split = int(len(dictionary) / self.clients) + 1

        list_of_chunks = []
        dict_items = list(dictionary.items())
        for i in range(0, len(dict_items), number_to_split):
            list_of_chunks.append(dict_items[i:i + number_to_split])
        result = []
        for index, chunk in enumerate(list_of_chunks):
            result.append([(key, value) for (key, value) in chunk])
        return result

    def exchange_data(self, splited_file, flag):
        map_result = []
        for worker_id, val in enumerate(splited_file):
            self.send_queue.put([flag, val])
        all_recived = False
        while not all_recived:
            try:
                data = self.recive_queue.get(timeout=30.0)
                map_result.append(data)
                if len(map_result) == len(splited_file):
                    all_recived = True
            except:
                print("Data not complete. Trying again")

        return map_result

    def await_start(self):
        print("Console started.")
        LOGGER.info(f"Console Started")
        while True:
            arg = input("> ")
            if arg == "start" and self.clients > 0:
                LOGGER.info(f"Activated start. Active clients:{self.clients}")
                print(f"Activated start. Active clients:{self.clients}")

                splited_file = self.split_file_by_lines(self.inputfile, self.clients)

                # map start
                map_file = self.exchange_data(splited_file, 0)

                # reshuffle
                shiffled_file = self.run_shuffle(map_file)

                # reduce start
                reduce_file = self.exchange_data(shiffled_file, 1)


                LOGGER.info(f"Result:{reduce_file}")
                print(f"Result:{reduce_file}")


            elif (self.clients == 0):
                print("No clients connected. Try again later.")
                LOGGER.error(f"No clients connected. Try again later.")

            else:
                print(f"Bad argument. Active clients:{self.clients}")

    def start(self):
        self.server.listen()
        LOGGER.info(f"[LISTENING] Server is listening on {self.SERVER}")

        console = threading.Thread(
            target=self.await_start, args=())
        console.start()
        while True:
            conn, addr = self.server.accept()
            client_UUID = conn.recv(36).decode(self.FORMAT)
            thread = threading.Thread(
                target=self.handle_client, args=(conn, addr, client_UUID))
            thread.start()
            self.clients = threading.active_count() - 2
            self.clients = threading.active_count() - 5
            LOGGER.info(f"[ACTIVE CONNECTIONS] {self.clients}")
            print(f"[ACTIVE CONNECTIONS] {self.clients}")


if __name__ == "__main__":
    inputfile = "'/Users/hubertkowalczyk/Documents/Studia/sem3/map_reduce/example.txt'"
    master_instance = Master(inputfile)
