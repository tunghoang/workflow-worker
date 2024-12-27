from osgeo import gdal, osr
import numpy as np
import glob, os, shutil
import datetime

from argparse import ArgumentParser
import sys


# sample command: python /home/ubuntu/workspace/software/pipeline/scripts/1.3.PrepDem/MAIN.py -i "/home/ubuntu/workspace/software/input_data/3.DEM" -o "/home/ubuntu/workspace/software/output_data/1.3.PrepDem"
###########################################################argparse
parser = ArgumentParser(description="Input Data")

parser.add_argument("-i", "--input",dest="input_folder", required=True)

parser.add_argument("-o", "--output",dest="output_folder", required=True)

parser.add_argument("-l", "--log",dest="logpath", required=True)

args = parser.parse_args()

##################################################################
import logging
logger = logging.getLogger('__1.3.PrepDEM__')
logging.basicConfig(filename= args.logpath, encoding='utf-8', level=logging.DEBUG)



##################################################################



input_folder = args.input_folder
output_folder = args.output_folder

input_path = input_folder + f'/DEM_ori.tif'
output_path = output_folder + f'/DEM_BTB_4km.tif'


def processRadar(input_path, output_path):

    
    

    if os.path.exists(input_path) and not os.path.exists(output_path):

        

        gdal.Warp(
                    output_path,
                    input_path,
                    format = "GTiff",
                    dstSRS = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs",
                    xRes = 0.04, yRes = -0.04,
                    outputBounds = (101, 17.5, 111, 21.1),
                    resampleAlg = "average",
                    creationOptions = ['COMPRESS=LZW'],
                )

        ds = gdal.Open(output_path, gdal.GA_Update)
        data = ds.ReadAsArray()
        data[data==32767] = 0

        band = ds.GetRasterBand(1)
        band.SetNoDataValue(-9999)
        band.WriteArray(data)

        ds = None

    if not os.path.exists(input_path):
        logger.warning(f"Tệp dữ liệu địa hình không tồn tại: {input_path}")
    if not os.path.exists(input_path) and len(list(glob.iglob(input_folder + f'/*.*'))) > 0:
        logger.warning(f"Tệp dữ liệu địa hình có thể không đúng theo định dạng: {input_path}")
     

if __name__ == '__main__':   
    processRadar(input_path, output_path)