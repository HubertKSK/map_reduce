#!/usr/bin/python

class Master(object):
    """docstring for Master."""

    def __init__(self, inputfile):
        super(Master, self).__init__()
        self.inputfile = inputfile
        print("starting Master")

if __name__ == "__main__":
    inputfile = "'/Users/hubertkowalczyk/Documents/Studia/sem3/map_reduce/testcode.py'"
    master_instance = Master(inputfile)
