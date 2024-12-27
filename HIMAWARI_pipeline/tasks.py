import os, glob, shutil
import traceback
import importlib.util
import json
import datetime
import sys
import time
import dateutil.parser as dparser
_cwd = os.path.dirname(__file__)
_pcwd = os.path.dirname(_cwd)
if _pcwd not in sys.path:
    sys.path.append(_pcwd)
if _cwd not in sys.path:
    sys.path.append(_cwd)

import config
import apis
import master
print(config.DATE_TIME)

os.environ["NO_ALBUMENTATIONS_UPDATE"] = "1"
from subprocess import Popen

import logging
logger = logging.getLogger('_MAIN_')
t_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
logpath = _cwd + '/' + f'{t_str}.log'
logging.basicConfig(filename=logpath, encoding='utf-8', level=logging.INFO)


#_DATE = config.DATE_TIME
#_tInfo = datetime.datetime.strptime(_DATE, '%Y%m%d')
#start_time = (_tInfo + datetime.timedelta(hours=0)).strftime('%Y%m%d_%H')
#end_time = (_tInfo + datetime.timedelta(hours=23)).strftime('%Y%m%d_%H')

def printArg(arg):
    print(arg)
    return [0]

def HimaPreprocess(startDate):
    stageName = 'HimaPreprocess'
    #checkInfo = apis.can_run(config.PIPELINE, stageName, dependOn, startDate)
    checkInfo = apis.can_run2(config.PIPELINE, stageName, startDate)
    if not checkInfo[0]:
        print("Cannot run because of " + str(checkInfo[1]))
        if checkInfo[1] == "Already run":
            master.enqueueNextJobs(config.PIPELINE, stageName, startDate)
        return
    rsl = []
    try:
        apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, 0)
        status = 0
        start_time_s = dparser.isoparse(startDate).strftime('%Y%m%d')
        start_time = datetime.datetime.strptime(start_time_s, '%Y%m%d')
        end_time = start_time + datetime.timedelta(hours=23)
    
        print('---------------------Hima preprocess ----------------------------')
        command = 'python {} -i {} -o {} -s {} -e {} -l {}'.format(
            config.PrepHima['scripts_path'],
            config.PrepHima['i'],
            config.PrepHima['o'],

            start_time.strftime('%Y%m%d_%H'),
            end_time.strftime('%Y%m%d_%H'),
            logpath
          )
        print(command)
        status = Popen(command, shell=True).wait()
        print(f'#status code: {status}')
        rsl.append(status)
        if status != 0:
          raise Exception(config.PIPELINE + " failed")
        if all(v == 0 for v in rsl):
            apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, 1)
            master.enqueueNextJobs(config.PIPELINE, stageName, startDate)
        else:
            apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, -1)
    except:
        print("Exception ........")
        apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, -2)
        
    return rsl

def DEMPreprocess(startDate):
    stageName = 'DEMPreprocess'
    #checkInfo = apis.can_run(config.PIPELINE, stageName, dependOn, startDate)
    checkInfo = apis.can_run2(config.PIPELINE, stageName, startDate)
    if not checkInfo[0]:
        print("Cannot run because of " + str(checkInfo[1]))
        if checkInfo[1] == "Already run":
            master.enqueueNextJobs(config.PIPELINE, stageName, startDate)
        return
    rsl = []
    try:
        apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, 0)
        status = 0
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

        rsl.append(status)
        if status != 0:
          raise Exception(config.PIPELINE + " failed")
        if all(v == 0 for v in rsl):
            apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, 1)
            master.enqueueNextJobs(config.PIPELINE, stageName, startDate)
        else:
            apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, -1)
    except:
        print("Exception ........")
        apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, -2)
        
    return rsl


def CaliHimawariOnly(startDate):
    stageName = 'CaliHimawariOnly'
    #checkInfo = apis.can_run(config.PIPELINE, stageName, dependOn, startDate)
    checkInfo = apis.can_run2(config.PIPELINE, stageName, startDate)
    if not checkInfo[0]:
        print("Cannot run because of " + str(checkInfo[1]))
        if checkInfo[1] == "Already run":
            master.enqueueNextJobs(config.PIPELINE, stageName, startDate)
        return
    rsl = []
    try:
        apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, 0)
        status = 0
        start_time_s = dparser.isoparse(startDate).strftime('%Y%m%d')
        start_time = datetime.datetime.strptime(start_time_s, '%Y%m%d')
        end_time = start_time + datetime.timedelta(hours=23)
        print('---------------------Calibrate Himawri-8 ----------------------------')
        command = 'python {} -i1 {} -i2 {} -o {} -s {} -e {} -l {}'.format(
            config.CaliHimaOnly['scripts_path'],
            config.CaliHimaOnly['i1'],
            config.CaliHimaOnly['i2'],
            config.CaliHimaOnly['o'],

            start_time.strftime('%Y%m%d_%H'),
            end_time.strftime('%Y%m%d_%H'),
            logpath,
        )
        print(command)
        status = Popen(command, shell=True).wait()
        print(f'#status code: {status}')

        rsl.append(status)
        if status != 0:
            raise Exception(config.PIPELINE + " failed")
        if all(v == 0 for v in rsl):
            apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, 1)
            master.enqueueNextJobs(config.PIPELINE, stageName, startDate)
        else:
            apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, -1)
    except:
        print("Exception ........")
        apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, -2)
        
    return rsl


def PostProcess(startDate):
    stageName = 'PostProcess'
    #checkInfo = apis.can_run(config.PIPELINE, stageName, dependOn, startDate)
    checkInfo = apis.can_run2(config.PIPELINE, stageName, startDate)
    if not checkInfo[0]:
        print("Cannot run because of " + str(checkInfo[1]))
        if checkInfo[1] == "Already run":
            master.enqueueNextJobs(config.PIPELINE, stageName, startDate)
        return
    rsl = []
    try:
        apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, 0)
        status = 0
        start_time_s = dparser.isoparse(startDate).strftime('%Y%m%d')
        start_time = datetime.datetime.strptime(start_time_s, '%Y%m%d')
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

            start_time.strftime('%Y%m%d'),
            logpath,
        )
        print(command)
        status = Popen(command, shell=True).wait()
        print(f'#status code: {status}')

        rsl.append(status)
        if status != 0:
            raise Exception(config.PIPELINE + " failed")
        if all(v == 0 for v in rsl):
            apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, 1)
            master.enqueueNextJobs(config.PIPELINE, stageName, startDate)
        else:
            apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, -1)
    except:
        print("Exception ........")
        apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, -2)
        
    return rsl
 
def ODCImport(startDate):
    stageName = 'ODCImport'
    #checkInfo = apis.can_run(config.PIPELINE, stageName, dependOn, startDate)
    checkInfo = apis.can_run2(config.PIPELINE, stageName, startDate)
    if not checkInfo[0]:
        print("Cannot run because of " + str(checkInfo[1]))
        if checkInfo[1] == "Already run":
            master.enqueueNextJobs(config.PIPELINE, stageName, startDate)
        return
    rsl = []
    try:
        apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, 0)
        status = 0
        start_date = dparser.isoparse(startDate)
        product = "deepModel"
        input_path = os.path.join(config.ODCImport['i'], "DATA_10KM_daily", product, str(start_date.year), str(start_date.month), str(start_date.day))
        command = f"python -m {config.ODCImport['module']} --path {input_path}" 
        print(command)
        status = Popen(command, shell=True).wait()
        print(f'#status code: {status}')

        rsl.append(status)
        if status != 0:
            raise Exception(config.PIPELINE + " failed")
        if all(v == 0 for v in rsl):
            apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, 1)
            master.enqueueNextJobs(config.PIPELINE, stageName, startDate)
        else:
            apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, -1)
    except:
        print("Exception ........")
        apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, -2)
        
    return rsl

def TerracottaImport(startDate):
    stageName = 'TerracottaImport'
    checkInfo = apis.can_run2(config.PIPELINE, stageName, startDate)
    if not checkInfo[0]:
        print("Cannot run because of " + str(checkInfo[1]))
        if checkInfo[1] == "Already run":
            master.enqueueNextJobs(config.PIPELINE, stageName, startDate)
        return
    rsl = []
    try:
        apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, 0)
        status = 0
        start_date = dparser.isoparse(startDate)
        product = "deepModel"
        input_path = os.path.join(config.TerracottaImport['i'], "DATA_10KM_daily", product, str(start_date.year), str(start_date.month), str(start_date.day))
        resolution = config.TerracottaImport['resolution']
        frequency = config.TerracottaImport['frequency']
        command = f"python -m {config.TerracottaImport['module']} --resolution {resolution} --frequency {frequency} --file {input_path}" 
        print(command)
        status = Popen(command, shell=True).wait()
        print(f'#status code: {status}')

        rsl.append(status)
        if status != 0:
            raise Exception(config.PIPELINE + " failed")
        if all(v == 0 for v in rsl):
            apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, 1)
            master.enqueueNextJobs(config.PIPELINE, stageName, startDate)
        else:
            apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, -1)
    except:
        print("Exception ........")
        traceback.print_exc()

        apis.upsert_pipeline_task(config.PIPELINE, stageName, startDate, -2)
        
    return rsl
'''
if __name__ == '__main__':

    log_start_time = time.time()

    product = "deepModel"
    HimaPreprocess()
    DEMPreprocess()
    CaliHimawariOnly()
    PostProcess()

    print("--- %s seconds ---" % (time.time() - log_start_time))
'''
