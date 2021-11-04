#!/usr/bin/python

class Slave(object):
    """docstring for Slave."""

    def __init__(self, inputfile):
        super(Slave, self).__init__()
        self.inputfile = inputfile
        print("starting Slave")

if __name__ == "__main__":
    inputfile = "'/Users/hubertkowalczyk/Documents/Studia/sem3/map_reduce/testcode.py'"
    master_instance = Slave(inputfile)
