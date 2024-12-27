import os, glob, shutil
import importlib.util
import json
import datetime
import sys
import time
_cwd, _=os.path.split(os.path.abspath(sys.argv[0]))

os.environ["NO_ALBUMENTATIONS_UPDATE"] = "1"
config_path = _cwd + '/' + 'config.py'
spec = importlib.util.spec_from_file_location("config", config_path)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)
from subprocess import Popen

import logging
logger = logging.getLogger('_MAIN_')
t_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
logpath = _cwd + '/' + f'{t_str}.log'
logging.basicConfig(filename=logpath, encoding='utf-8', level=logging.INFO)


_DATE = config.DATE_TIME
_tInfo = datetime.datetime.strptime(_DATE, '%Y%m%d')
start_time = (_tInfo + datetime.timedelta(hours=0)).strftime('%Y%m%d_%H')
end_time = (_tInfo + datetime.timedelta(hours=23)).strftime('%Y%m%d_%H')


def HimaPreprocess():
    print('---------------------Hima preprocess ----------------------------')
    command = 'python {} -i {} -o {} -s {} -e {} -l {}'.format(
        config.PrepHima['scripts_path'],
        config.PrepHima['i'],
        config.PrepHima['o'],

        start_time,
        end_time,
        logpath
      )
    print(command)
    status = Popen(command, shell=True).wait()
    print(f'#status code: {status}')



def DEMPreprocess():
    print('---------------------DEM preprocess ----------------------------')
    command = 'python {} -i {} -o {} -l {}'.format(
        config.PrepDem['scripts_path'],
        config.PrepDem['i'],
        config.PrepDem['o'],

        logpath,

      )
    print(command)
    status = Popen(command, shell=True).wait()
    print(f'#status code: {status}')



def CaliHimawariOnly():
    print('---------------------Calibrate Himawri-8 ----------------------------')
    command = 'python {} -i1 {} -i2 {} -o {} -s {} -e {} -l {}'.format(
        config.CaliHimaOnly['scripts_path'],
        config.CaliHimaOnly['i1'],
        config.CaliHimaOnly['i2'],
        config.CaliHimaOnly['o'],

        start_time,
        end_time,
        logpath,
      )
    print(command)
    status = Popen(command, shell=True).wait()
    print(f'#status code: {status}')


def PostProcess():
    print('-----------------Convert to 10x10km, daily------------------')
    # AWS
    # deepModel
    # integrated
    # IMERG_Final
    # FY4A
    # GSMaP
    # PERSIANN_CCS
    # Radar
    product = "deepModel"
    input_folder = config.PostProc['i7']

    command = 'python {} -i {} -o {} -p {} -t {} -l {}'.format(
        config.PostProc['scripts_path'],
        input_folder,
        config.PostProc['o'],
        product,

        _DATE,
        logpath,
      )
    print(command)
    status = Popen(command, shell=True).wait()
    print(f'#status code: {status}')


if __name__ == '__main__':

    log_start_time = time.time()

    product = "deepModel"
    HimaPreprocess()
    DEMPreprocess()
    CaliHimawariOnly()
    PostProcess()

    print("--- %s seconds ---" % (time.time() - log_start_time))
