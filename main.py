#!/usr/bin/python

import getopt
import sys
from pathlib import Path
from classes.master_class import Master
from classes.slave_class import Slave


def main(argv):
    inputfile = ''
    flag = ''
    try:
        opts, args = getopt.getopt(argv, "hi:f:", ["ifile=", "flag="])
    except getopt.GetoptError:
        print('main.py -f <flag:slave/master> -i <inputfile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('main.py -f <flag:slave/master> -i <inputfile>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            try:
                arg = Path(arg).resolve(strict=True)
            except FileNotFoundError:
                # doesn't exist
                print(f"ERROR\nFILE {arg} NOT EXIST")
                exit()
            else:
                # exists
                inputfile = arg
        elif opt in ("-f", "--flag"):
            if arg == 'master' or arg == 'slave':
                flag = arg
            else:
                print("ERROR\nBad flag")
                exit()
    print(f'flag:{flag}')
    print(f"Input_file:'{inputfile}'")
    if flag == 'master':
        master_instance = Master(inputfile)
    else:
        slave_instance = Slave()
        slave_instance.listen()
        slave_instance.listen()
        slave_instance.dissconect()


if __name__ == "__main__":
    main(sys.argv[1:])
