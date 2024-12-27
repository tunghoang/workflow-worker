print("Ckeck ....")
from netCDF4 import Dataset
from struct import unpack
from numpy import array,asarray,arange,flipud,dtype
import numpy as np
import os, shutil, glob
import time
import sys
module_path=os.path.join(os.path.dirname(__file__))
if str(module_path) not in sys.path:
    sys.path.append(str(module_path))

import _readdataSat

import datetime
from osgeo import gdal, osr
import pandas as pd
#import patoolib


def create_Satnc(fileout, lon, lat, utctime, image,calibration):
    file = Dataset(fileout,'w')
    file.title = "Netcdf statellite product - Made by AMO"
    file.createDimension('latitude',1125)
    file.createDimension('longitude',1125)
    file.createDimension('time',1)
    times = file.createVariable('time',dtype('q').char,('time',))
    lons = file.createVariable('longitude',dtype('f4').char,('longitude',))
    lats = file.createVariable('latitude',dtype('f4').char,('latitude',))
    lats.units = 'degrees_north'
    lats.standard_name = "Latitude"
    lats.long_name = "Latitude"
    lats.axis = "Y"
    lons.standard_name = "Longitude"
    lons.long_name = "Longitude"
    lons.axis = "X"
    lons.units = 'degrees_east'
    times.standard_name = "UTC time"
    times.axis = "T"
    lons[:] = lon
    lats[:] = lat
    times[:] = utctime
    image_ = file.createVariable('image',dtype('i4').char,('longitude','latitude'),fill_value=255, zlib=True, least_significant_digit=4)
    image_.units = 'RGB (0 - 255)'
    image_[:] = image
    calibration_ = file.createVariable('calibration',dtype('f4').char,('longitude','latitude'),fill_value=-9.e+33, zlib=True, least_significant_digit=4)
    calibration_.units = '(K) or (%) tuy thuoc loai anh'
    calibration_[:] = calibration
    file.close()

def read_Satnc(path):
    fh = Dataset(path, mode='r')
    for key in fh.variables:
        # print(key)
        if(key != "calibration" and key != "latitude" and  key != "longitude" and key != "time"):
            key_data = key #image
    data = fh.variables[key_data][:] #image raw
    cali = fh.variables["calibration"][:]
    databb= cali #value temple
    lats_data = fh.variables["latitude"][:]
    lons_data = fh.variables["longitude"][:]
    namefile = os.path.basename(path)
    year = namefile.split("_")[1][0:4]
    month = namefile.split("_")[1][4:6]
    day = namefile.split("_")[1][6:8]
    hour = namefile.split(".")[1][1:3]
    minute = namefile.split(".")[1][3:5]
    return lats_data,lons_data, databb, data, year, month, day, hour, minute


def changeBandName(new_name):
    # VS1A, VSB ~ VSB ~ B03B
    # N1B ~ B04B
    # N2B ~ B05B
    # N3B ~ B06B
    # I4B ~ B07B !
    # WVB ~ B08B !
    # W2B ~ B09B
    # W3B ~ B10B
    # MIB ~ B11B
    # O3B ~ B12B
    # IRB ~ B13B !
    # L2B ~ B14B
    # I2B ~ B15B !
    # COB ~ B16B
    match new_name:
        case "N1B":
            return "B04B"
        case "N2B":
            return "B05B"
        case "N3B":
            return "B06B"
        # case "I4B":
        #     return "I4B"
        # case "WVB":
        #     return "WVB"
        case "W2B":
            return "B09B"
        case "W3B":
            return "B10B"
        case "MIB":
            return "B11B"
        case "O3B":
            return "B12B"
        # case "IRB":
        #     return "IRB"
        case "L2B":
            return "B14B"
        # case "I2B":
        #     return "I2B"
        case "COB":
            return "B16B"
        case _:
            return new_name


def reverseChangeBandName(old_name):

    match old_name:
        case "B04B":
            return "N1B"
        case "B05B":
            return "N2B"
        case "B06B":
            return "N3B"
        case "B09B":
            return "W2B"
        case "B10B":
            return "W3B"
        case "B11B":
            return "MIB"
        case "B12B":
            return "O3B"
        case "B14B":
            return "L2B"
        case "B16B":
            return "COB"
        case _:
            return old_name


def save2tiff(data, timeInfo, output_folder, band_name):
    # data: numpy.ma.maskArray

    year = str(timeInfo.year).zfill(4)
    month = str(timeInfo.month).zfill(2)
    day = str(timeInfo.day).zfill(2)
    hour = str(timeInfo.hour).zfill(2)
    minute = str(timeInfo.minute).zfill(2)

    # folderpath = output_folder + f'/{band_name}/{year}/{month}/{day}'
    folderpath = output_folder

    if not os.path.exists(folderpath):
        os.makedirs(folderpath)

    # fname = f'radar_{year}{month}{day}_{hour}.tif'
    fname = f'{band_name}_{year}{month}{day}.Z{hour}{minute}.tif'

    tif_fpath = folderpath + '/' + fname

    

    # flip/masked data
    if data.mask == False:
        fill_v = data.fill_value

        data = data.data
        data = np.flip(data, 0)
        #nodata
        data[data==fill_v] = -9999
    else:
        data = None

    dst_ds = gdal.GetDriverByName('GTiff').Create(tif_fpath, data.shape[1], data.shape[0], 1, gdal.GDT_Float32, options=['COMPRESS=LZW'])
    outRasterSRS = osr.SpatialReference()
    outRasterSRS.ImportFromEPSG(4326)
    dst_ds.SetProjection(outRasterSRS.ExportToWkt())
    dst_ds.SetGeoTransform((94.98, 0.04, 0, 39.98, 0, -0.04))
    dst_ds.GetRasterBand(1).WriteArray(data)
    dst_ds.GetRasterBand(1).SetNoDataValue(-9999)
    dst_ds.FlushCache()
    dst_ds=None

    return tif_fpath


def bin2tif(filepath, output_folder):
    namefile = filepath
    lats_data,lons_data, data_temple, data_image, year, month, day, hour, minute = _readdataSat.readdataSat(filepath)
    create_Satnc(namefile.split(".")[0]+"."+namefile.split(".")[1] +".nc", lons_data, lats_data,str(year)+str(month)+str(day)+str(hour)+str(minute), data_image,data_temple)
    lats_data, lons_data, data_temple, data_image, year, month, day, hour, minute = read_Satnc(namefile.split(".")[0]+"."+namefile.split(".")[1] +".nc")

    ### test
    # _fpath = namefile.split(".")[0]+"."+namefile.split(".")[1] +".nc"
    # __fpath = f'NETCDF:"{_fpath}":calibration'
    # data1 = gdal.Open(__fpath, gdal.GA_ReadOnly).ReadAsArray()
    # data = data_temple.data
    # data = np.flip(data, 0)
    # print(np.all(data1 == data))
    ###

    _, filename = os.path.split(filepath)
    timeInfo = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute))

    band_name = filename.split('_')[0]

    band_name = changeBandName(band_name) #new name to old name

    tif_fpath = save2tiff(data_temple, timeInfo, output_folder, band_name)

    os.remove(namefile.split(".")[0]+"."+namefile.split(".")[1] +".nc") # remove nc file

    return tif_fpath



def getTimeInfo(filename):
    year = int(filename.split('_')[1][0:4])
    month = int(filename.split('_')[1][4:6])
    day = int(filename.split('_')[1][6:8])
    hour = int(filename.split('.')[1][1:3])
    minute = int(filename.split('.')[1][3:5])

    return datetime.datetime(year, month, day, hour, minute)


def resampleBTB(filepath, output_folder):
    _, filename = os.path.split(filepath)

    band_name = filename.split('_')[0]
    timeInfo = getTimeInfo(filename)

    year = str(timeInfo.year).zfill(4)
    month = str(timeInfo.month).zfill(2)
    day = str(timeInfo.day).zfill(2)
    hour = str(timeInfo.hour).zfill(2)
    minute = str(timeInfo.minute).zfill(2)

    folderpath = output_folder + f'/{band_name}/{year}/{month}/{day}'

    if not os.path.exists(folderpath):
        os.makedirs(folderpath)


    fname = f'{band_name}_{year}{month}{day}.Z{hour}{minute}.BTB.tif'
    tif_fpath = folderpath + '/' + fname

    gdal.Warp(
                    tif_fpath,
                    filepath,
                    format = "GTiff",
                    dstSRS = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs",
                    xRes = 0.04, yRes = -0.04,
                    outputBounds = (101, 17.5, 111, 21.1),
                    resampleAlg = "average",
                    creationOptions = ['COMPRESS=LZW'],
    )


def processAFile(file_path, output_folder, temporal_folder):

    try:
        region_fpath = bin2tif(file_path, temporal_folder) # bin2tif
    except Exception as e:
        return False, str(e)

    resampleBTB(region_fpath, output_folder)

    os.remove(region_fpath)

    return True, ""
    

def img_process_mins(input_folder, output_folder, time_str, logger):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        
    temporal_folder = output_folder

    
    date_part, time_part = time_str.split("_")
    # COB_20241114.Z0000
    for file_path in glob.iglob(input_folder + '/' + f'*_{date_part}.Z{time_part}00'):
        print(file_path)
        flag, status = processAFile(file_path, output_folder, temporal_folder)

        if not flag:
            logger.warning(f"input file: {file_path} does not exist or is corrupted")
