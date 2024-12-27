import os, sys
import datetime
####################################argparse
def is_valid_directory(parser, arg):
    if not os.path.isdir(arg):
        print('The directory {} does not exist! create it'.format(arg))
        os.makedirs(arg)

    return arg
'''
def is_valid_directory(parser, arg):
    if not os.path.isdir(arg):
        parser.error('The directory {} does not exist!'.format(arg))
    else:
        # File exists so return the directory
        return arg
'''

def is_valid_datetime(parser, arg):
    try:
        return datetime.datetime.strptime(arg, '%Y%m%d')
        # return arg
    except:
        parser.error('-s: The date string is error!')

def is_valid_file(parser, arg):
    if not os.path.isfile(arg):
        parser.error('The file {} does not exist!'.format(arg))
    else:
        # File exists so return the directory
        return arg

