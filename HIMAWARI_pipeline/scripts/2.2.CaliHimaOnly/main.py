import pandas as pd
import numpy as np
import os, glob, shutil
import datetime

# from sklearn.ensemble import RandomForestClassifier
import joblib
import cv2

from dataloader import BuildingsDataset, get_features_transform, get_target_transform, BuildingsDatasetML

import segmentation_models_pytorch as smp
import torch
from osgeo import gdal, osr

import time
from argparse import ArgumentParser
import sys


_cwd, _=os.path.split(os.path.abspath(sys.argv[0]))
torch.hub.set_dir(_cwd)

#sample command # python main.py -i1 "/home/ubuntu/workspace/indra/truongnx/data/himaBTB" -i2 "/home/ubuntu/workspace/indra/truongnx/data/himaBTB" -s "20200918_15" -e "20200918_15" -o "maps" -l log.txt
#sample command # python main.py -i1 "/home/ubuntu/workspace/software/output_data/1.1.PrepHima/hourly" -i2 "/home/ubuntu/workspace/software/output_data/1.3.PrepDem" -s "20201018_00" -e "20201018_00" -o "/home/ubuntu/workspace/software/output_data/2.2.CaliHimaOnly" -l log.txt
###########################################################argparse
parser = ArgumentParser(description="Input Data")

parser.add_argument("-i1", "--input1",dest="hima_folderpath", required=True)

parser.add_argument("-i2", "--input2",dest="dem_folderpath", required=True)

parser.add_argument("-s", "--input3",dest="stime_str", required=True)

parser.add_argument("-e", "--input4",dest="etime_str", required=True)

parser.add_argument("-o", "--output",dest="output_folderpath", required=True)

parser.add_argument("-l", "--log",dest="logpath", required=True)

args = parser.parse_args()
##########################################################################
hima_folder = args.hima_folderpath
dem_folder = args.dem_folderpath

# time_str = args.time_str

# timeInfo = datetime.datetime.strptime(time_str, '%Y%m%d_%H')

start_date = args.stime_str
end_date = args.etime_str
start_time = datetime.datetime.strptime(start_date, '%Y%m%d_%H')
end_time = datetime.datetime.strptime(end_date, '%Y%m%d_%H')

output_folder = args.output_folderpath

############################################################################
# start_time = datetime.datetime(2020, 9, 18, 15)
# end_time = datetime.datetime(2020, 9, 18, 16)


# input_folder = r'/home/ubuntu/workspace/indra/truongnx/data/himaBTB'

# output_folder = r'maps'
###############################################################################
BASE_DIR, _ = os.path.split(os.path.abspath(sys.argv[0]))


class_mdl_path = BASE_DIR + r'/resources/RF_2_class.pkl'
reg_mdl_path = BASE_DIR + r'/resources/model_last_99.pth'


class_features = ['wvb_value','DEM', 'b09_b11', 'b14_i2b', 'irb_b16', 'wvb_b10', 'i2b_b16', 'b10_b12',
                    'b11_irb', 'b10_b16', 'i4b_b12', 'b12_b16', 'b11_b14',  'irb_b14' , 'i4b_irb']

################## deep model
reg_features = ['b09_value', 'b10_value', 'b11_value', 'b12_value', 'b14_value', 'b16_value', 'i2b_value', 'i4b_value', 'irb_value', 'wvb_value', 'DEM']

maxmin_csv_path = BASE_DIR + '/resources/maxminstat.csv'
maxmin_rf_path = BASE_DIR + '/resources/maxminstat_rf.csv'


def crop_image(image, target_image_dims=[90,250]):

    target_x = target_image_dims[0]
    target_y = target_image_dims[1]
    rows, cols = image.shape
    padd_x = (rows - target_x) // 2
    padd_y = (cols - target_y) // 2


    return image[
        padd_x:rows - padd_x,
        padd_y:cols - padd_y,
    ]

def createMap(img, outfpath):

    output_raster = gdal.GetDriverByName('GTiff').Create(outfpath, 250, 90, 1 ,gdal.GDT_Float32)  # Open the file
    output_raster.SetGeoTransform((101.0, 0.04, 0.0, 21.1, 0.0, -0.04))
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    output_raster.SetProjection( srs.ExportToWkt())
    band = output_raster.GetRasterBand(1)
    band.SetNoDataValue(-9999)
    band.WriteArray(img)

    output_raster.FlushCache()


test_dataset = BuildingsDataset(
    hima_folder,
    dem_folder,
    reg_features,
    start_time,
    end_time,
    maxmin_csv_path,
)


ml_dataset = BuildingsDatasetML(
    hima_folder,
    dem_folder,
    class_features,
    start_time,
    end_time,
    maxmin_csv_path
)

###################################
n_channels = len(reg_features)
n_classes = 1

model = smp.DeepLabV3Plus(
    encoder_name="resnet34",
    in_channels=n_channels,
    classes=n_classes,
)

model.load_state_dict(torch.load(reg_mdl_path, map_location=torch.device('cpu'), weights_only=True))

model.eval()
#####################################

clf = joblib.load(class_mdl_path)

#####################################

current = start_time
idx = 0
while current <= end_time:
    year = str(current.year).zfill(4)
    month = str(current.month).zfill(2)
    day = str(current.day).zfill(2)

    ##############################Class
    image = ml_dataset[idx]
    _C, _W, _H = image.shape
    image = image.reshape((_C, _W*_H))
    image = image.transpose()

    cls_output = clf.predict(image)
    cls_output = cls_output.reshape((_W, _H))


    ############################### Deep
    image = test_dataset[idx]

    tensor = image.unsqueeze(0)

    with torch.no_grad():
        output = model(tensor)

    img = output[0][0].detach().numpy()

    img = crop_image(img)

    img = img * (260 - 0.2) + 0.2

    img[cls_output==0] = 0

    ##### img
    ####################################
    out_folder = output_folder + f'/{year}/{month}/{day}'
    if not os.path.exists(out_folder):
        os.makedirs(out_folder)
    outfpath = out_folder + '/' + f'deepModel_{current.strftime("%Y%m%d%H0000")}.tif' # deepModel_20201013000000.tif
    createMap(img, outfpath)
    ####################################

    idx = idx + 1
    current = current + datetime.timedelta(hours=1)
