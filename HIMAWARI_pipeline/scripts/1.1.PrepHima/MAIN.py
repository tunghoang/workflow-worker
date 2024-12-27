#from rehima import img_process_mins
from reHima.reHima import img_process_mins
from newtb1h import img_process_hourly
from argparse import ArgumentParser
import sys
import datetime

# sample command: python /home/ubuntu/workspace/software/pipeline/scripts/1.1.PrepHima/MAIN.py -i "/home/ubuntu/workspace/software/input_data/1.Himawari8" -s "20201018_04" -e "20201018_04" -o "/home/ubuntu/workspace/software/output_data/1.1.PrepHima"
###########################################################argparse
parser = ArgumentParser(description="Input Data")

parser.add_argument("-i", "--input",dest="input_folderpath", required=True)


parser.add_argument("-s", "--input2",dest="stime_str", required=True)

parser.add_argument("-e", "--input3",dest="etime_str", required=True)

parser.add_argument("-o", "--output",dest="output_folderpath", required=True)

parser.add_argument("-l", "--log",dest="logpath", required=True)

args = parser.parse_args()

##################################################################
import logging
logger = logging.getLogger('__1.1.PrepHima__')
logging.basicConfig(filename= args.logpath, encoding='utf-8', level=logging.INFO)


##################################################################

input_folder = args.input_folderpath
output_folder = args.output_folderpath
stime_str = args.stime_str
etime_str = args.etime_str

if __name__ == '__main__':
    #################################
    ########### MAIN ################
    #################################
    mins_folder = output_folder + '/mins'
    hourly_folder = output_folder + '/hourly'

    start_date = datetime.datetime.strptime(stime_str, '%Y%m%d_%H')
    end_date = datetime.datetime.strptime(etime_str, '%Y%m%d_%H')

    current = start_date
    while (current <= end_date):

        time_str = current.strftime('%Y%m%d_%H')

        img_process_mins(input_folder, mins_folder, time_str, logger)

        img_process_hourly(mins_folder, hourly_folder, time_str)

        current = current + datetime.timedelta(hours=1)
