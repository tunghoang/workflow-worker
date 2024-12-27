import datetime
import os

PIPELINE='HIMAWARI_pipeline'

#############################################################################

DATE_TIME = '20241116' #YYYYMMDD

BASE_DIR = os.path.dirname(__file__)

OPTION = 6
# 1 Radar-Gauge
# 2 Rain Product-IMERG Final
# 3 Rain Product-PERSIANN CCS
# 4 Rain Product-GSMaP
# 5 Rain Product-FenYun
# 6 Calibrate Himawari
# 7 Integrate Rain data

##############################################################################
SCRIPTS_DIR = BASE_DIR + '/scripts'
print(SCRIPTS_DIR)
OUTPUT_DIR = BASE_DIR + '/tmp/output_data'


PrepHima = {
	'scripts_path' : SCRIPTS_DIR + r'/1.1.PrepHima/MAIN.py',
	'i' : '/himawari',
	'o' : OUTPUT_DIR + r'/1.1.PrepHima',
}


PrepDem = {
	'scripts_path' : SCRIPTS_DIR + r'/1.3.PrepDem/MAIN.py',
	'i' : '/data/pipeline/input/DEM',
	'o' : OUTPUT_DIR + r'/1.3.PrepDem',
}




CaliHimaOnly = {
	'scripts_path' : SCRIPTS_DIR + r'/2.2.CaliHimaOnly/main.py',
	'i1' : OUTPUT_DIR + r'/1.1.PrepHima/hourly',
	'i2' : OUTPUT_DIR + r'/1.3.PrepDem',
	'o' : OUTPUT_DIR + r'/2.2.CaliHimaOnly',
}


PostProc = {
	'scripts_path' : SCRIPTS_DIR + r'/4.1.PostProc/MAIN.py',

	'i7' : OUTPUT_DIR + r'/2.2.CaliHimaOnly',
	'o' : '/data/DATA',

}
ODCImport = {
    'module': 'indra.add_dataset',
    'i': '/data/DATA'
}
TerracottaImport = {
    'module': 'tc_import',
    'i': '/data/DATA',
    'resolution': '10',
    'frequency': 'daily'
}
