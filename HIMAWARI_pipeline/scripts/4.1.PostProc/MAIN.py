import os
from osgeo import gdal, osr
from tqdm import tqdm
from datetime import datetime, timedelta
import cv2
import numpy as np
import pandas as pd
from pathlib import Path
gdal.DontUseExceptions()

from argparse import ArgumentParser
import sys




# python /home/ubuntu/workspace/software/pipeline/scripts/4.1.PostProc/MAIN.py -p "GSMaP" -i "/home/ubuntu/workspace/software/output_data/1.4.PrepRainP/GSMaP/GSMaP_BTB_4KM_1h" -o "/home/ubuntu/workspace/software/output_data/4.1.PostProc" -t "20201014"
###########################################################argparse
parser = ArgumentParser(description="Input Data")

parser.add_argument("-i", "--input1",dest="input_folderpath", required=True)

parser.add_argument("-t", "--input2",dest="time_str", required=True)

parser.add_argument("-o", "--output",dest="output_folderpath", required=True)

parser.add_argument("-p", "--product",dest="product", required=True)

parser.add_argument("-l", "--log",dest="logpath", required=True)

args = parser.parse_args()


##################################################################
import logging
logger = logging.getLogger('__4.1.PostProcessing__')
logging.basicConfig(filename= args.logpath, encoding='utf-8', level=logging.INFO)


##################################################################

product = args.product
src_dir = args.input_folderpath
dst_dir = args.output_folderpath
time_str = args.time_str


def extract_product_datetime(file_name: str):
    # shorten file name
    file_name = file_name.split('.')[0].split('_')[-1]
    return datetime.strptime(file_name, '%Y%m%d%H%M%S')

def generate_product_output(dt: datetime, product: str):
    return dt.strftime(f'%Y/%m/%d/{product}_%Y%m%d%H%M%S.tif')

def mkdir(path):
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
        
def process_resample(product, src_dir, dst_dir, time_str=None):
    src_dir = Path(src_dir)
    #dst_dir = os.path.join(Path(dst_dir), f'{product}_BTB_10KM_1h')
    dst_dir = os.path.join(Path(dst_dir), 'DATA_10KM_hourly', f'{product}')
    # generate list files in input dir
    list_file = [(path, file_name) 
                    for path, dirs, files in os.walk(src_dir) 
                    for file_name in files 
                    if os.path.splitext(file_name)[-1] in ['.tif', '.nc']
                    if file_name.rsplit('_', 1)[0] == product]
    list_file.sort()

    for path, src_name in tqdm(list_file):

        dt = extract_product_datetime(src_name)
        src_path = os.path.join(path, src_name)
        dst_path = os.path.join(dst_dir, generate_product_output(dt, product))

        if time_str:
            time = datetime.strptime(time_str, "%Y%m%d")
            if (dt - timedelta(hours=1)).replace(hour=0) != time:
                continue

        mkdir(dst_path)
        gdal.Warp(
            dst_path,
            src_path,
            format = 'GTiff',
            dstSRS = "+proj=longlat + ellps=WGS84 + datum=WGS84 + no_defs",
            xRes = 0.1, yRes = 0.1,
            outputBounds = (101, 17.5, 111, 21.1),
            resampleAlg = "average",
            creationOptions = ['COMPRESS=LZW'],
        )
    
    return dst_dir

def export_daily(dst_dir, paths, date, product):
    # Kiểm tra sự đầy đủ của dữ liệu hàng giờ
    if len(paths) < 12: #1
        logger.error("Không đủ thời gian để tổng hợp!")
        return False

    # define meta variables
    driver = gdal.GetDriverByName('GTiff')
    projection = osr.SpatialReference()
    projection.ImportFromEPSG(4326)
    projection = projection.ExportToWkt()
    geotransform = 101, 0.1, 0, 21.1, 0, -0.1
    nodata = - float('inf')
    
    img = np.array([cv2.imread(path, -1).astype(float) for path in paths])
    img = img.sum(axis=0)
    img[img < 0] = nodata
    dst_path = os.path.join(dst_dir, generate_product_output(date, product))
    mkdir(dst_path)
    
    
    
    dst_data = driver.Create(
        dst_path,
        img.shape[1],
        img.shape[0],
        1,
        gdal.GDT_Float32,
        options=['COMPRESS=LZW'],
    )
    
    dst_data.SetProjection(projection)
    dst_data.SetGeoTransform(geotransform)
    dst_data.GetRasterBand(1).WriteArray(img)
    dst_data.GetRasterBand(1).SetNoDataValue(nodata)
    dst_data.FlushCache()
    dst_data = None

    ### save to csv
    print('!!!!save to csv')
    rsl = []
    rows, cols = img.shape
    for i in range(rows):
        for j in range(cols):
            lat = 101 + 0.1 * j
            lon = 21.1 - 0.1 * i
            if img[i][j] > 0:
                rsl.append([i, j, lat, lon, img[i][j]])

    df = pd.DataFrame(rsl, columns = ['row', 'col', 'lat', 'lon', 'value'])
    df.to_csv(dst_path.replace('.tif', '.csv'), index=False)
    

def process_todaily(product, src_dir, dst_dir, time_str=None):
    src_dir = Path(src_dir)
    #dst_dir = os.path.join(Path(dst_dir), f'{product}_BTB_10KM_1d')
    dst_dir = os.path.join(Path(dst_dir), 'DATA_10KM_daily', f'{product}')
    # generate list files in input dir
    list_file = [(path, file_name) 
                    for path, dirs, files in os.walk(src_dir) 
                    for file_name in files 
                    if os.path.splitext(file_name)[-1] == '.tif'
                    if file_name.rsplit('_', 1)[0] == product]
    list_file.sort()

    cache = []
    last_date = datetime(2010, 12, 31)
    for path, src_name in tqdm(list_file):
        current_date = extract_product_datetime(src_name) - timedelta(hours=1)
        current_date = current_date.replace(hour=0)
        src_path = os.path.join(path, src_name)
        
        if time_str:
            time = datetime.strptime(time_str, "%Y%m%d")
            if current_date != time:
                continue
            
        if current_date != last_date:

            export_daily(dst_dir, cache, last_date, product)
            cache.clear()
            
        # cập nhật danh sách tạm thời
        cache.append(src_path)
        last_date = current_date
    
    export_daily(dst_dir, cache, last_date, product)


def post_process(product, src_dir, dst_dir, time_str=None):
    path = process_resample(product, src_dir, dst_dir, time_str)
    process_todaily(product, path, dst_dir, time_str)

if __name__ == '__main__':
    # AWS
    # deepModel
    # integrated
    # IMERG_Final
    # FY4A
    # GSMaP
    # PERSIANN_CCS
    # Radar

    
    post_process(product, src_dir, dst_dir, time_str)

    # check output
    #dst_dir = os.path.join(Path(dst_dir), f'{product}_BTB_10KM_1d')
    est_dir = os.path.join(Path(dst_dir), f'DATA_10KM_daily', f'{product}v_10KM_daily')
    dst_path = os.path.join(dst_dir, generate_product_output(datetime.strptime(time_str, "%Y%m%d"), product))
    if os.path.exists(dst_path):
        logger.info(f"Tạo sản phẩm mưa {product} 10x10 km BTB theo ngày thành công!")
    else:
        logger.error(f"Tạo sản phẩm mưa {product} 10x10 km BTB theo ngày thất bại!")
    # check

