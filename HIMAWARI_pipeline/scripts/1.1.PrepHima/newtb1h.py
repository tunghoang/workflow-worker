from osgeo import gdal, osr
import numpy as np
import os, glob, shutil
import datetime
import pandas as pd


# input_folder = '/home/ubuntu/workspace/software/output_data/1.1.PrepHima/mins'
# output_folder = '/home/ubuntu/workspace/software/output_data/1.1.PrepHima/hourly'


# time_str = "20201018_04"


def load_data(list_files):
    tensor = []
    for fpath in list_files:
        data = gdal.Open(fpath, gdal.GA_ReadOnly).ReadAsArray()
        data[data <= -9999] = np.nan
        tensor.append(data)
    return np.array(tensor)

def img_process_hourly(input_folder, output_folder, time_str):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    list_bands = ['B09B', 'B10B', 'B11B', 'B12B', 'B14B', 'B16B', 'I2B', 'I4B', 'IRB', 'WVB']

    timeInfo = datetime.datetime.strptime(time_str, '%Y%m%d_%H')
    

    s_time = timeInfo - datetime.timedelta(minutes=50)

    for band in list_bands:

        list_files = []

        c_time = s_time
        while c_time <= timeInfo:
            year = str(c_time.year).zfill(4)
            month = str(c_time.month).zfill(2)
            day = str(c_time.day).zfill(2)
            hour = str(c_time.hour).zfill(2)
            minute = str(c_time.minute).zfill(2)

            filepath = input_folder + f'/{band}/{year}/{month}/{day}/{band}_{year}{month}{day}.Z{hour}{minute}.BTB.tif'
            print(filepath)
            if os.path.exists(filepath):
                list_files.append(filepath)

            c_time = c_time + datetime.timedelta(minutes=10)
        print(list_files)
        if len(list_files) > 0:



            tensor = load_data(list_files)

            output_matrix = np.nanmean(tensor, axis = (0))

            o_year = str(timeInfo.year)
            o_month = str(timeInfo.month).zfill(2)
            o_day = str(timeInfo.day).zfill(2)
            o_hour = str(timeInfo.hour).zfill(2)

            outfname = '{}_{}{}{}.Z{}00_TB.tif'.format(band, o_year, o_month, o_day, o_hour)

            hourly_folderpath = output_folder + '/{}/{}/{}/{}'.format(band, o_year, o_month, o_day)
            if not os.path.exists(hourly_folderpath):
                os.makedirs(hourly_folderpath)

            hourly_tif_fpath = hourly_folderpath + '/' + outfname

            # print(hourly_tif_fpath)
            src_ds = gdal.Open(list_files[0], gdal.GA_ReadOnly)
            driver = gdal.GetDriverByName('GTiff')
            outRaster = driver.Create(hourly_tif_fpath, output_matrix.shape[1], output_matrix.shape[0], 1, gdal.GDT_Float32, options=['COMPRESS=LZW'])
            outRaster.SetGeoTransform(src_ds.GetGeoTransform())
            outband = outRaster.GetRasterBand(1)
            outband.WriteArray(output_matrix)
            outRasterSRS = osr.SpatialReference()
            outRasterSRS.ImportFromEPSG(4326)
            outRaster.SetProjection(outRasterSRS.ExportToWkt())
            outband.FlushCache()
