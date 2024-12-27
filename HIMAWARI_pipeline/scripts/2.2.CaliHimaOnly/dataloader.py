import torch
import albumentations as album
import cv2
import pandas as pd
import numpy as np
import datetime

def getImgPaths(hima_folder, dem_folder, year, month, day, hour, bands = None):
    rsl = []

    year = str(year).zfill(4)
    month = str(month).zfill(2)
    day = str(day).zfill(2)
    hour = str(hour).zfill(2)

    # lấy data hima
    rsl = []
    for band in bands:
        if band == 'DEM':
            filepath = dem_folder + '/DEM_BTB_4km.tif'
            rsl.append([filepath])
        else:
            if 'value' in band:
                band_name = band.split('_')[0].upper()
                if band_name[0] == 'B':
                    band_name2 = band_name + 'B'
                else:
                    band_name2 = band_name
                filepath = hima_folder + '/' + f'{band_name2}/{year}/{month}/{day}/{band_name2}_{year}{month}{day}.Z{hour}00_TB.tif'
                rsl.append([filepath])
            else:

                first = band.split('_')[0]
                second= band.split('_')[1]

                band_name = first.upper()
                if band_name[0] == 'B':
                    band_name2 = band_name + 'B'
                else:
                    band_name2 = band_name
                filepath1 = hima_folder + '/' + f'{band_name2}/{year}/{month}/{day}/{band_name2}_{year}{month}{day}.Z{hour}00_TB.tif'

                band_name = second.upper()
                if band_name[0] == 'B':
                    band_name2 = band_name + 'B'
                else:
                    band_name2 = band_name
                filepath2 = hima_folder + '/' + f'{band_name2}/{year}/{month}/{day}/{band_name2}_{year}{month}{day}.Z{hour}00_TB.tif'

                rsl.append([filepath1, filepath2])

    return rsl


####
def readImg(list_paths, target_reg=False):
    rsl = []
    for ele in list_paths:
        if len(ele) == 1:
            filepath = ele[0]
            img = cv2.imread(filepath, cv2.IMREAD_UNCHANGED)
        else:
            filepath0 = ele[0]
            img0 = cv2.imread(filepath0, cv2.IMREAD_UNCHANGED)
            filepath1 = ele[1]
            img1 = cv2.imread(filepath1, cv2.IMREAD_UNCHANGED)

            img = img0 - img1


        rsl.append(img)

    return rsl


###
def applyNorm(image, maxmin, bands):

    #maxmin feature, max, min

    rsl = []
    for band_name, matrix2D in zip(bands, image):
        # print(band_name)
        # print(matrix2D)
        _name, _min, _max = maxmin[maxmin['feature'] == band_name].values[0]
        matrix2D = (matrix2D - _min)/ (_max - _min)
        rsl.append(matrix2D)

    return rsl


def get_features_transform(for_target = False):

    transform = [
        album.PadIfNeeded(min_height=96, min_width=256, always_apply=True, border_mode=0, value = 0),
    ]
    return album.Compose(transform)

def get_target_transform(for_target = False):
    transform = [
    album.PadIfNeeded(min_height=96, min_width=256, always_apply=True, border_mode=cv2.BORDER_CONSTANT, value = -9999),
    ]
    return album.Compose(transform)

def get_training_augmentations():
    train_transform1 = [
        album.HorizontalFlip(p=1)
    ]

    train_transform2 = [
        album.VerticalFlip(p=1)
    ]

    return [album.Compose(train_transform1), album.Compose(train_transform2)]


class BuildingsDataset(torch.utils.data.Dataset):

    def __init__(
            self,
            hima_folder,
            dem_folder,
            bands,
            start_time,
            end_time,
            maxmin_csv_path,
    ):

        self.bands = bands

        self.maxmin = pd.read_csv(maxmin_csv_path)

        self.image_paths = []

        self.feature_preprocessing = get_features_transform()


        current = start_time
        while current <= end_time:
            year = current.year
            month = current.month
            day = current.day
            hour = current.hour

            img_paths = []
            data_paths = getImgPaths(hima_folder, dem_folder, year, month, day, hour, bands)


            self.image_paths.append(data_paths)

            current = current + datetime.timedelta(hours = 1)

    def __getitem__(self, i):
        pass
        # # # read images and masks
        image = readImg(self.image_paths[i])


        # # apply max min normalization
        image = applyNorm(image, self.maxmin, self.bands)

        # # apply preprocessing (padding 0)
        image = np.array(image, dtype =np.float32)
        image = image.transpose((1, 2, 0))   # CHW -> HWC


        if self.feature_preprocessing:
            sample = self.feature_preprocessing(image=image)
            image = sample['image']


        # # change dimensions
        image = image.transpose(2, 0, 1).astype('float32') # HWC -> CHW

        # # to tensor
        image = torch.as_tensor(image)

        return image

    def __len__(self):
        # return length of
        return len(self.image_paths)


def BuildingsDatasetML(hima_folder, dem_folder, bands, start_time, end_time, maxmin_csv_path):

    maxmin = pd.read_csv(maxmin_csv_path)

    image_paths = []

    current = start_time
    while current <= end_time:
        year = current.year
        month = current.month
        day = current.day
        hour = current.hour

        img_paths = []
        data_paths = getImgPaths(hima_folder, dem_folder, year, month, day, hour, bands)


        image_paths.append(data_paths)

        current = current + datetime.timedelta(hours = 1)

    output_arr = []
    for ele in image_paths:
        arr = readImg(ele)
        arr = applyNorm(arr, maxmin, bands)
        output_arr.append(arr)

    return np.array(output_arr)
