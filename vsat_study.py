#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import gzip
import glob
import time
import json
import logging
import logging.handlers
import subprocess
import locale
import shutil

from datetime import datetime, timedelta, date
from time import strptime, gmtime, strftime
from threading import Thread
from pytz import timezone
from shutil import copy2

import pyVSAT
import pyCRED

# create logger
path_log = r'C:\VSAT\logger'
log = logging.getLogger('VSAT')
log.setLevel(logging.DEBUG)
log_file = os.path.join(path_log, 'log_vsat.txt')
handler = logging.handlers.TimedRotatingFileHandler(log_file, backupCount=10, when='midnight')
handler.setFormatter(logging.Formatter('%(sistema)s %(asctime)s %(levelname)9s: %(message)s'))
handler.setLevel(logging.DEBUG)
log.addHandler(handler)

pathmain = os.path.dirname(os.path.abspath(__file__))
path_data = os.path.join(pathmain,'escenarios')
path_rawgz = r'C:\VSAT\raw_se'

system = {0:'SIN', 1:'BCA', 2:'BCS'}


def cargabilidad(sis):
    """
    Programa principal para calcular la maxima transferencia de potencia en compuestas del SIN
    """
    log_extra = {'sistema': system[sis]}
    log.info('Inicia programa de calculo de maxima transferencia de potencia', extra=log_extra)
    param = pyCRED.psw()

    # SE LEE ARCHIVO DE CONFIGURACION
    try:
        with open(os.path.join(r'C:\VSAT','parametros_'+system[sis]+'.cfg')) as f:
            cfg = json.load(f)
    except Exception as e:
        log.error('Error al leer archivo de configuración: %s'%e, extra = log_extra)
        return 0

    # DESCARGA RAW DE SERVIDOR
    # log.info('Inicia descarga de archivo RAW de servidor SFTP', extra = log_extra)
    # fecha_fin = datetime.now() 
    # fecha_ini = fecha_fin-timedelta(minutes=10)
    # list_files = pyVSAT.get_raw_fromSFTP(param.serv_sftp[sis], '', path_rawgz, list_filenames=[], 
    #              rgx=r'.*\.RAW.gz', date_ft=[fecha_ini, fecha_fin], log_sys=system[sis])
    # gzfile = os.path.join(path_rawgz, list_files[0])
    # gzfile = r'C:\VSAT\raw_se\17Jul2018_075928.RAW.gz'

    # if not os.path.isfile(gzfile):
    #     log.error('No se descargo archivo RAW de servidor SFTP', extra = log_extra)
    #     return 0

    # DESCOMPRIME ARCHIVO raw.gz
    # gzfile = r'C:\RAW_SE\18Jul2018_090857.RAW.gz'
    # rawfile = pyVSAT.gz2raw(gzfile, log_sys=system[sis])
    # rawfile = r'C:\RAW_SE\01Ago2018_121908.RAW'
    # rawfile = r'C:\Users\usuario_uia\Desktop\09Ago2018_094216\09Ago2018_094216.RAW'
    rawfile = r'C:\Users\O30164\23Ago2018_085859.RAW'

    # CREA CARPETA DE REPORTE CON FECHA RAW COMO NOMBRE Y COPIA RAW ORIGINAL A ESCENARIO
    fecha = pyVSAT.get_datetime_raw(rawfile, sistema=sis, rpl_tz=False, log_sys=system[sis])
    fecha_s = pyVSAT.date2string(fecha)
 
    path_fecha = os.path.join(path_data, system[sis], fecha_s)
    log.info('Creando carpeta: %s'%path_fecha, extra=log_extra)
    if not os.path.exists(path_fecha):
        os.makedirs(path_fecha)
    else:
        shutil.rmtree(path_fecha, True)
        os.makedirs(path_fecha)

    c_rawfile = os.path.join(path_fecha, os.path.basename(rawfile))
    copy2(rawfile, c_rawfile)

    # MODIFICA LIMITE HLIMH DE GENERADORES (10000MW)
    if cfg["MOD_HLIM"]==1:
        rawfile = pyVSAT.mod_lim(c_rawfile, log_sys=system[sis])

    # SE CREA ARCHIVO PFB
    info_raw = pyVSAT.procesa_raw(rawfile, path_fecha, log_sys=system[sis])
    pfbfile = pyVSAT.run_psat(info_raw[0], cfg["PATHPSAT"], log_sys=system[sis])

    # SE GENERAN ARCHIVOS VSAT (COMPUERTAS)
    data = pyVSAT.read_enl_mdb(sistema=system[sis])  
    enlaces = [item['ENLACE'].replace('/', '-')+'_'+item['SENTIDO'] for item in data if not 'CURVAS' in item['ENLACE']]
    pyVSAT.create_batch(path_fecha, enlaces, log_sys=system[sis])
    pyVSAT.vsat_flowgates(path_fecha, info_raw, pfbfile, cfg, log_sys=system[sis])

    # EJECUTA VSAT
    pyVSAT.run_vsat(path_fecha, path_data, cfg["PATHVSATBATCH"], log_sys=system[sis])

    # Guarda resultados en Base de Datos
    pyVSAT.save_lim(path_fecha, enlaces, sys_num=sis)

    # COMPRIME CARPETA DE ESENARIOS
    # pyVSAT.zip_7z(path_fecha, remove_orig=False, log_sys=system[sis]) 

    # FIN
    log.info('Fin de proceso \n', extra=log_extra)

    return 1



if __name__ == '__main__':
    if len(sys.argv) > 1:
        sis = int(sys.argv[1])
    else:
        sis = 0 # 0:'SIN', 1:'BCA', 2:'BCS'

    r = cargabilidad(sis)
