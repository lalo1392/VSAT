#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Librerias de programa VSA (vsat_study.py)
"""
__author__ = "Boris Alcaide"
__copyright__ = "CENACE, 2018"
__credits__ = ["Boris Alcaide", "Miguel Chavez", "Hugo Vega"]
__version__ = "0.0.1"
__maintainer__ = "Boris Alcaide"
__email__ = "boris.alcaide@cenace.gob.mx"
__status__ = "Prototype" 


# Libreries
import os
import re
import sys
import csv
import time
import glob
import gzip
import locale
import shutil
import pymongo
import zipfile
import logging
import paramiko
import subprocess
import logging.handlers
import numpy as np
import pandas as pd

from pytz import timezone
from zipfile import ZipFile
from pymongo import MongoClient
from os import path, remove, close
from time import strptime, gmtime, strftime
from datetime import datetime, timedelta, date

import pyCRED

log = logging.getLogger('VSAT')

def get_raw_fromSFTP(serv_sftp, directory, pathD, list_filenames=[], rgx=r'.*\.*', date_ft=[], log_sys='SIN'):
    """
    * Copia los archivos de servidor SFTP
    """
    log_extra = {'sistema': log_sys}

    try:
        t = paramiko.Transport((serv_sftp['server'], serv_sftp.get('port', 22)))
    except paramiko.ssh_exception.SSHException, e:
        log.warning('No se pudo conectar al servidor: [%s]'%str(e), extra = log_extra)
        return 1

    try:
        t.connect(username=serv_sftp['usr'], password=serv_sftp['pwd'])
    except paramiko.ssh_exception.AuthenticationException, e:
        log.warning('Fallo la autenticacion: [%s]'%str(e), extra = log_extra)
    
    sftp = paramiko.SFTPClient.from_transport(t)

    try:
        sftp.chdir(directory)
    except IOError, e:
        log.warning('No se encontro el directorio: [%s]'%str(e), extra = log_extra)
        return 1

    if list_filenames:
        list_files = list_filenames
    else:
        all_files = sftp.listdir('.')
        regex = re.compile(rgx)
        list_files = filter(regex.search, all_files)

        if len(date_ft)==2:
            list_date_files = [] 
            for file in list_files:
                timestamp = sftp.stat(file).st_mtime
                mdate = datetime.fromtimestamp(timestamp)
                if (mdate>date_ft[0]) and (mdate<date_ft[1]):
                    list_date_files.append(os.path.basename(file))

            list_files = list_date_files
    
    d_files = []
    for file in list_files[-1:]:
        try:
            log.info('Descargando de servidor SFTP archivo: [%s]'%file, extra = log_extra)
            sftp.get(file, os.path.join(pathD,file))
            d_files.append(file)
        except Exception as e:
            log.warning('No se descargo archivo, Error: %s'%str(e), extra = log_extra)

    sftp.close()
    t.close()

    time.sleep(2)

    return d_files


def gz2raw(archivo, log_sys='SIN'):
    """
    Descomprime el gz de forma temporal.
    Regresa el nombre del archivo temporal
    """
    log_extra = {'sistema': log_sys}
    log.info('Descomprime archivo: %s'%archivo, extra = log_extra)
    folder = os.path.dirname(archivo)
    gzF = gzip.open(archivo, 'rb')
    outFname = os.path.join(folder, os.path.splitext(os.path.basename(archivo))[0])
    outF = open(outFname, 'wb')
    outF.flush()
    os.fsync(outF)
    outF.write(gzF.read())
    gzF.close()
    outF.close()

    return outF.name


def get_datetime_raw(fecha_str, sistema=0, rpl_tz=True, log_sys='SIN'):
    """
    Obtener el la fecha/hora de un archivo raw como objeto datetime
    """
    log_extra = {'sistema': log_sys}
    TIME_ZONES = {0: timezone('America/Mexico_City'),
              1: timezone('America/Tijuana'),
              2: timezone('Mexico/BajaSur')}

    RGX_RAWFILE = re.compile(r'\d{2}[a-z,A-Z]{3}\d{4}_?\d*')

    print RGX_RAWFILE

    locale.setlocale(locale.LC_ALL, 'esp_esp')
    tz = TIME_ZONES[sistema]

    bn = RGX_RAWFILE.search(fecha_str).group(0)
    fecha2 = list(bn)
    fecha2[2] = fecha2[2].lower()
    fecha2 = ''.join(fecha2)
    nchars = len(fecha2)

    fmt = '%d%b%Y'
    if nchars > 10:
        fmt += '_%H'
    if nchars > 12:
        fmt += '%M%S'
    
    try:
        fecha = datetime(*strptime(fecha2, fmt)[:6])
        locale.setlocale(locale.LC_ALL, 'english_us')
        if rpl_tz:
            fecha_local = tz.localize(fecha).astimezone(timezone('America/Mexico_City'))
            return fecha_local.replace(tzinfo=None)
        else:
            return fecha
    except Exception as e:
        log.error('No se pudo convertir la fecha del raw: %s : %s'%(e.__class__.__name__, e), extra = log_extra)
        fecha = None


def zip_7z(source, remove_orig=True, log_sys='SIN'):  
    """
    Comprime archivo o folder especificado a formato 7z
    """
    log_extra = {'sistema': log_sys}
    log.info('Comprimiendo carpeta de resultados: %s'%source, extra = log_extra)
    if path.isfile(source):
        folder=False
    elif path.isdir(source):
        folder=True
    else:
        print 'Error en PATH source'
        return 0

    target = path.join(path.dirname(source), path.splitext(path.basename(source))[0]+'.7z')
    out = subprocess.check_output("C:\\VSAT\\7za.exe" + " a -t7z \"" + target + "\" \"" + source + "\" -mx=9")
   
    if 'Everything is Ok' in out:
        if remove_orig:
            if folder==False:
                os.remove(source)
            else:
                shutil.rmtree(source, True)
    else:
        log.error('No se creo archivo/carpeta *.7z correctamente', extra = log_extra)
    
        return 0

    return target


def mod_lim(file_raw, log_sys='SIN'):
    """
    Abre limites de generadores 
    """
    log_extra = {'sistema': log_sys}
    log.info('Se modifican limites de generadores ', extra = log_extra)

    flag_gens = 'END OF GENERATOR DATA'
    s = open(file_raw,'r')

    mod_file_raw = os.path.join(os.path.dirname(file_raw), 
                          'mod_'+os.path.splitext(os.path.basename(file_raw))[0])+'.RAW'
    tmpfile = open(mod_file_raw,'w')
    
    flag = 0
    for i, line in enumerate(s.readlines()):   
        linea = line.split(',')
        if 'BEGIN GENERATOR DATA' in line:
            inicia_gen = i+1
        
        if flag_gens in line:
            flag = 1
        
        try:
            if i >= inicia_gen and flag == 0:
                linea[16] = 10000.0
                line = ','.join(str(w) for w in linea)
        except:
            pass
    
        tmpfile.write(line)

    s.close()
    tmpfile.close()
    
    return mod_file_raw    


def find_label(filename, label1, label2, label3, label4, label5, label6):
    with open(filename) as myfile:
        for num, line in enumerate(myfile, 1):
            if label1 in line:
                indice1 = num
            elif label2 in line:
                indice2 = num
            elif label3 in line:
                indice3 = num
            elif label4 in line:
                indice4 = num
            elif label5 in line:
                indice5 = num
            elif label6 in line:
                indice6 = num
                break
    return indice1, indice2, indice3, indice4, indice5, indice6


def procesa_raw(file_raw, Ruta, log_sys='SIN'):
    """
    Programa para modificar parametros de archivo RAW
    Eliminar ramas paralelas con mismo ID de archivo RAW
    """
    log_extra = {'sistema': log_sys}
    log.info('Inicia Funcion para eliminar ramas paralelas con mismo ID de archivo RAW', extra = log_extra)

    file = open(file_raw, "r")
    ruta_lista = file_raw.split('\\')
    file_raw_nuevo = os.path.join(Ruta, 'New_'+ruta_lista[-1])
    raw = file.read()
    raw_filas = np.array(raw.split('\n'))

    # ETIQUETAS
    lab_bus = '0 / END OF BUS DATA, BEGIN LOAD DATA'
    lab_load = '0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA'
    lab_genI = '0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA'
    lab_genF = '0 / END OF GENERATOR DATA, BEGIN BRANCH DATA'
    lab_branch = '0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA'
    lab_area = '0 / END OF TRANSFORMER DATA, BEGIN AREA DATA'

    bus_end, load_end, gen_ini, gen_end, brch_end, area_ini = find_label(file_raw, lab_bus, lab_load, lab_genI,
                                                                         lab_genF, lab_branch, lab_area)

    # Lee las 3 lineas del encabezado
    encabezado_raw = np.array(raw_filas[0:3])
    # Lee seccion Buses
    buses_raw = np.array(raw_filas[3:bus_end-1])
    # Lee seccion Cargas
    cargas_raw = np.array(raw_filas[bus_end:load_end-1])
    # Lee Fixed Shunt data
    fixed_shunt_raw = np.array(raw_filas[load_end:gen_ini-1])
    # Lee seccion Generadores
    generadores_raw = np.array(raw_filas[gen_ini:gen_end-1])
    # Lee la seccion de lineas
    lineas_raw = np.array(raw_filas[gen_end:brch_end-1])
    # Lee Transformadores
    trafos_raw = np.array(raw_filas[brch_end:area_ini-1])
    # Lee seccion Areas
    areas_raw = np.array(raw_filas[area_ini::])

    ramas_sep = []
    nodos_env = []
    nodos_rec = []
    IDs = []
    z_ramas = []
    v1 = np.arange(len(lineas_raw))
    v1 = v1[::-1]
    k = 0
    for j in v1:
        pos_e = []
        pos_r = []
        flag = 0
        ramas_sep.append(lineas_raw[j].split(','))
        n_env = ramas_sep[k][0]
        n_rec = ramas_sep[k][1]
        iden = ramas_sep[k][2]
        impedancia = ramas_sep[k][3]
        if n_env == n_rec:
            log.info('Se elimino rama con ID repetido: %s'%lineas_raw[j][:22], extra = log_extra)
            lineas_raw = np.delete(lineas_raw, j, None)
        elif n_env in nodos_env:
            for idx, item in enumerate(nodos_env):
                if item == n_env:
                    pos_e.append(idx)
            for m in pos_e:
                if n_rec == nodos_rec[m]:
                    if iden == IDs[m]:
                        if impedancia <= z_ramas[m]:
                            log.info('Se elimino rama con ID repetido: %s'%lineas_raw[j][:22], extra = log_extra)
                            lineas_raw = np.delete(lineas_raw, j, None)
                        else:
                            log.info('Se elimino rama con ID repetido: %s'%lineas_raw[len(lineas_raw)-m-1][:22], extra = log_extra)
                            lineas_raw = np.delete(lineas_raw, len(lineas_raw)-m-1, None)
                            nodos_env.remove(nodos_env[m])
                            nodos_rec.remove(nodos_rec[m])
                            IDs.remove(IDs[m])
                            z_ramas.remove(z_ramas[m])
                            nodos_env.append(n_env)
                            nodos_rec.append(n_rec)
                            IDs.append(iden)
                            z_ramas.append(impedancia)
                        flag = 1
                        break
            if flag == 1:
                k += 1
                continue
            else:
                nodos_env.append(n_env)
                nodos_rec.append(n_rec)
                IDs.append(iden)
                z_ramas.append(impedancia)
        elif n_env in nodos_rec:
            for idx, item in enumerate(nodos_rec):
                if item == n_env:
                    pos_r.append(idx)  # First time seeing the element
            for n in pos_r:
                if n_rec == nodos_env[n]:
                    if iden == IDs[n]:
                        if impedancia <= z_ramas[n]:
                            log.info('Se elimino rama con ID repetido: %s'%lineas_raw[j][:22], extra = log_extra)
                            lineas_raw = np.delete(lineas_raw, j, None)
                        else:
                            log.info('Se elimino rama con ID repetido: %s'%lineas_raw[len(lineas_raw)-n-1][:22], extra = log_extra)
                            lineas_raw = np.delete(lineas_raw, len(lineas_raw) - n - 1, None)
                            nodos_env.remove(nodos_env[n])
                            nodos_rec.remove(nodos_rec[n])
                            IDs.remove(IDs[n])
                            z_ramas.remove(z_ramas[n])
                            nodos_env.append(n_env)
                            nodos_rec.append(n_rec)
                            IDs.append(iden)
                            z_ramas.append(impedancia)
                        flag = 1
                        break
            if flag == 1:
                k += 1
                continue
            else:
                nodos_env.append(n_env)
                nodos_rec.append(n_rec)
                IDs.append(iden)
                z_ramas.append(impedancia)
        else:
            nodos_env.append(n_env)
            nodos_rec.append(n_rec)
            IDs.append(iden)
            z_ramas.append(impedancia)
        k += 1
    file_new = open(file_raw_nuevo, "w")

    raw_nuevo = np.append(encabezado_raw, buses_raw)
    raw_nuevo = np.append(raw_nuevo, lab_bus)
    raw_nuevo = np.append(raw_nuevo, cargas_raw)
    raw_nuevo = np.append(raw_nuevo, lab_load)
    raw_nuevo = np.append(raw_nuevo, fixed_shunt_raw)
    raw_nuevo = np.append(raw_nuevo, lab_genI)
    raw_nuevo = np.append(raw_nuevo, generadores_raw)
    raw_nuevo = np.append(raw_nuevo, lab_genF)
    raw_nuevo = np.append(raw_nuevo, lineas_raw)
    raw_nuevo = np.append(raw_nuevo, lab_branch)
    raw_nuevo = np.append(raw_nuevo, trafos_raw)
    raw_nuevo = np.append(raw_nuevo, lab_area)
    raw_nuevo = np.append(raw_nuevo, areas_raw)

    file_new.writelines("%s\n" % item for item in raw_nuevo)
    file.close()
    file_new.close()

    if 'mod' in os.path.splitext(os.path.basename(file_raw))[0]:
        mod_file_raw = file_raw
    else:
        mod_file_raw = os.path.join(Ruta,'mod_'+os.path.splitext(os.path.basename(file_raw))[0]+'.RAW')

    os.remove(file_raw)
    os.rename(file_raw_nuevo, mod_file_raw)

    return [mod_file_raw, buses_raw, cargas_raw, generadores_raw, lineas_raw, trafos_raw, fixed_shunt_raw]



def run_psat(rawfile, PSATPATH, log_sys='SIN'):
    """
    Crea rchivo PFB
    """
    log_extra = {'sistema': log_sys}
    log.info('Inicia Funcion crear archivo PFB', extra = log_extra)

    # Rutas de archivos
    pfbfile =  os.path.join(os.path.dirname(rawfile), os.path.splitext(os.path.basename(rawfile))[0]+'.pfb')
    scriptpath = os.path.join(os.path.dirname(rawfile), 'psat_script.py')

    # Crea archivo SCRIPT de PSAT DSAtools python       
    text_file = open(scriptpath, "w")
    text_file.write("""from psat_python import *\n""")
    text_file.write("""error = psat_error()\n""")
    text_file.write("""psat_command('Import:"%s";PTI Rawd 32', error)\n"""%(rawfile.replace(os.path.sep, '/')))
    text_file.write("""psat_command('SetSolutionAlgorithm:NR', error)\n""")
    text_file.write("""psat_command('SetSolutionParameter:FlatStart;FLAT', error)\n""")
    text_file.write("""psat_command('Solve', error)\n""")
    text_file.write("""psat_command('SavePowerflowAs:"%s"', error)""" %(pfbfile.replace(os.path.sep, '/')))
    text_file.close()

    # Elimina archivo pfb si existe. Si existe archivo pfb no se lanza el proceso de PSAT.exe
    if (os.path.isfile(pfbfile)): 
        os.remove(pfbfile)

    #  Ejecuta script
    subprocess.STARTF_USESHOWWINDOW = 1
    subprocess.SW_SHOWMINNOACTIVE = 7
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = subprocess.SW_SHOWMINNOACTIVE

    cmd = PSATPATH + " \"" + scriptpath + "\" python"
    pPSAT = subprocess.Popen(cmd, startupinfo=startupinfo)

    # Revisa si la tarea de PSAT ya termino
    timeC = 0
    while (timeC < 20):
        timeC+=0.5
        if (pPSAT.poll() == None):
            time.sleep(0.5)
            if (os.path.isfile(pfbfile)):
                time.sleep(1)
                pPSAT.terminate()
                break

    return pfbfile


def create_batch(path_fecha, enlaces, log_sys='SIN'):
    """
    Crea archivo maestro VSAT con todos los escenarios a ejecutarse
    """
    log_extra = {'sistema': log_sys}
    log.info('Creando archivo maestro VSAT con todos los escenarios a ejecutarse', extra = log_extra)

    with open(os.path.join(path_fecha, 'vsat_batch.dat'),'a') as f:
        for enl in enlaces:
            if not 'CURVAS_CAPABILIDAD' in enl:
                f.write('1\n') 
                f.write(enl+'\n') 
                f.write(enl+'.snr\n') 
                f.write('\n')


def vsat_flowgates(path_fecha, info_raw, pfbfile, cfg, log_sys='SIN'):
    """
    Crea archivos de configuracion de compuertas para VSAT
    """
    log_extra = {'sistema': log_sys}
    log.info('Creando archivos de configuracion de compuertas para VSAT', extra = log_extra)

    # GENERAR LOS DICCIONARIOS A PARTIR DEL RAW 
    raw_nuevo, buses_raw, cargas_raw, generadores_raw, lineas_raw, trafos_raw, fixed_shunt_raw = info_raw
    bus_dict = {}
    bus_dict_aux = {}
    gen_load_dict = {}
    brns_dict = {}
    fxs_dict = {}
    for l, line in enumerate(buses_raw):    # DICCIONARIO 1
        datos = line.split(',')
        kvs = datos[2].strip()
        area = datos[4].strip()
        siglas = datos[1][1:4]
        if '.00' in kvs:
            kvs = str(int(float(kvs)))
        else:
            kvs = str(round(float(kvs), 2))
        if int(datos[4].strip()) < 10:
            area = '0' + area
        clave = area + siglas + '-' + kvs
        bus_dict_aux[datos[0].strip()] = clave
        if clave in bus_dict:
            bus_dict[clave].append(datos[0].strip())
        else:
            bus_dict[clave] = [datos[0].strip()]

    for l, line in enumerate(cargas_raw):   # DICCIONARIO 2
        datos = line.split(',')
        num = datos[0].strip()
        iden = datos[1]
        clave = bus_dict_aux[num] + 'L' + iden.strip().strip('\'').strip()
        gen_load_dict[clave] = num + ' ' + iden
    for l, line in enumerate(generadores_raw):
        datos = line.split(',')
        num = datos[0].strip()
        iden = datos[1]
        clave = bus_dict_aux[num] + 'G' + iden.strip().strip('\'').strip()
        gen_load_dict[clave] = num + ' ' + iden

    for l, line in enumerate(lineas_raw):   # DICCIONARIO 3
        datos = line.split(',')
        num_e = datos[0].strip()
        num_r = datos[1].strip()
        iden = datos[2]
        clave = bus_dict_aux[num_e] + ' ' + bus_dict_aux[num_r] + ' ' + iden
        brns_dict[clave] = num_e + ' ' + num_r + ' ' + iden
        clave_i = bus_dict_aux[num_r] + ' ' + bus_dict_aux[num_e] + ' ' + iden
        brns_dict[clave_i] = num_r + ' ' + num_e + ' ' + iden
    for l, line in enumerate(trafos_raw):
        if l == 0 or l % 4 == 0:
            datos = line.split(',')
            num_e = datos[0].strip()
            num_r = datos[1].strip()
            iden = datos[3]
            clave = bus_dict_aux[num_e] + ' ' + bus_dict_aux[num_r] + ' ' + iden
            brns_dict[clave] = num_e + ' ' + num_r + ' ' + iden
            clave_i = bus_dict_aux[num_r] + ' ' + bus_dict_aux[num_e] + ' ' + iden
            brns_dict[clave_i] = num_r + ' ' + num_e + ' ' + iden

    for l, line in enumerate(fixed_shunt_raw):   # DICCIONARIO 4
        datos = line.split(',')
        num = datos[0].strip()
        iden = datos[1]
        clave = bus_dict_aux[num] + 'F' + iden.strip().strip('\'').strip()
        fxs_dict[clave] = num + ' ' + iden

    # LEER EL MACHOTE DE CADA COMPUERTA
    db_compuerta = read_enl_mdb(log_sys)
    dict_curvas = db_compuerta[90]
    curvas = dict_curvas['DATA']

    file_reporte = open(os.path.join(path_fecha,'Cambios_Etiquetas.txt'), 'w')
    for ite in range(len(db_compuerta)):
        print 'Enlace: %s'%(db_compuerta[ite]['ENLACE'])
        scenario = []
        parameter = []
        criteria = []
        monitoreo = []
        contingencia = []
        interface = []
        transferencia = []
        sps = []
        remedial_control = []
        tipo = ['SNR', 'PRM', 'CRT', 'MON', 'CTG', 'ITF', 'TRF', 'SPS', 'RMC']
        if db_compuerta[ite]['ENLACE'] == "CURVAS_CAPABILIDAD":
            new_curvas = []
            cn = 0          # OMITIR INFORMACION DE LOS GENERADORES NO ENCONTRADOS
            for c_line in curvas:
                if cn > 0 and cn <= 7:
                    cn += 1
                    continue
                if '&' in c_line:
                    cn = 0
                    indices = [i for i, c in enumerate(c_line) if c == "&"]
                    gen_gcc = c_line[indices[0] + 1:indices[1]]
                    if gen_gcc in gen_load_dict:
                        c_line = c_line.replace('&%s&' % gen_gcc, gen_load_dict[gen_gcc])
                        file_reporte.write('En el archivo GCC hubo cambio de %s por %s \n'
                                            % (gen_gcc, gen_load_dict[gen_gcc]))
                    else:
                        cn += 1
                        new_curvas.pop()
                        continue
                new_curvas.append(c_line)
        else:
            dict_compuerta = db_compuerta[ite]
            flow = dict_compuerta['SENTIDO']
            for k, val in enumerate(tipo):
                try:
                    archivo = dict_compuerta[val]
                except:
                    continue
                for a, a_line in enumerate(archivo[0]):
                    if val == 'SNR':
                        if 'Powerflow File' in a_line:
                            a_line = a_line % raw_nuevo
                        elif 'PFB File' in a_line:
                            a_line = a_line % pfbfile
                        scenario.append(a_line)
                    elif val == 'PRM':
                        claves_prm = []
                        if 'Exclude branches below kV level' in a_line:
                            a_line = a_line % cfg['EXCLUDE_KV']
                        elif 'Use generator capability curves' in a_line:
                            a_line = a_line % cfg['CAPABILITY']
                        elif '$' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "$"]
                            for j in range(0, len(indices), 2):
                                k = j + 1
                                claves_prm.append(a_line[indices[j] + 1:indices[k]])
                            for m in range(len(claves_prm)):
                                if claves_prm[m] in bus_dict: 
                                    a_line = a_line.replace('$%s$' % claves_prm[m], ', '.join(bus_dict[claves_prm[m]]))
                                    file_reporte.write('En el archivo CRT hubo cambio de %s por %s \n'
                                                    % (claves_prm[m], ', '.join(bus_dict[claves_prm[m]])))
                                else:
                                    file_reporte.write('NO SE ENCONTRO LA CLAVE %s \n' % claves_prm[m])
                                    continue
                        parameter.append(a_line)
                    elif val == 'CRT':
                        claves_crt = []
                        if '$' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "$"]
                            for j in range(0, len(indices), 2):
                                k = j + 1
                                claves_crt.append(a_line[indices[j] + 1:indices[k]])
                            for m in range(len(claves_crt)):
                                if claves_crt[m] in bus_dict: 
                                    a_line = a_line.replace('$%s$' % claves_crt[m], ', '.join(bus_dict[claves_crt[m]]))
                                    file_reporte.write('En el archivo CRT hubo cambio de %s por %s \n'
                                                        % (claves_crt[m], ', '.join(bus_dict[claves_crt[m]])))
                                else:
                                    file_reporte.write('NO SE ENCONTRO LA CLAVE %s \n' % claves_crt[m])
                                    continue
                        elif 'High' in a_line:
                            a_line = a_line % cfg['H_LIM']
                        elif 'Low' in a_line:
                            a_line = a_line % cfg['L_LIM']
                        criteria.append(a_line)
                    elif val == 'MON':
                        claves_mon = []
                        gen_mon = []
                        if '$' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "$"]
                            for j in range(0, len(indices), 2):
                                k = j + 1
                                claves_mon.append(a_line[indices[j] + 1:indices[k]])
                            for m in range(len(claves_mon)):
                                if claves_mon[m] in bus_dict:
                                    a_line = a_line.replace('$%s$' % claves_mon[m], ', '.join(bus_dict[claves_mon[m]]))
                                    file_reporte.write('En el archivo MON hubo cambio de %s por %s \n'
                                                        % (claves_mon[m], ', '.join(bus_dict[claves_mon[m]])))
                                else:
                                    file_reporte.write('NO SE ENCONTRO LA CLAVE %s \n' % claves_mon[m])
                                    continue
                        elif '&' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "&"]
                            gen_mon.append(a_line[indices[0] + 1:indices[1]])
                            if gen_mon[0] in gen_load_dict:
                                a_line = a_line.replace('&%s&' % gen_mon[0], gen_load_dict[gen_mon[0]])
                                file_reporte.write('En el archivo MON hubo cambio de %s por %s \n'
                                               % (gen_mon[0], gen_load_dict[gen_mon[0]]))
                            else:
                                file_reporte.write('En el archivo MON NO hubo cambio de %s \n'
                                               % gen_mon[0])
                                continue
                        monitoreo.append(a_line)
                    elif val == 'CTG':
                        buses_ctg = []
                        brn_ctg = []
                        gen_ctg = []
                        fx_ctg = []
                        if '$' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "$"]
                            for j in range(0, len(indices), 2):
                                k = j + 1
                                buses_ctg.append(a_line[indices[j] + 1:indices[k]])
                            for m in range(len(buses_ctg)):
                                a_line = a_line.replace('$%s$' % buses_ctg[m], ', '.join(bus_dict[buses_ctg[m]]))
                                file_reporte.write('En el archivo CTG hubo cambio de %s por %s \n'
                                                   % (buses_ctg[m], ', '.join(bus_dict[buses_ctg[m]])))
                        elif '#' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "#"]
                            brn_ctg.append(a_line[indices[0] + 1:indices[1]])
                            a_line = a_line.replace('#%s#' % brn_ctg[0], brns_dict[brn_ctg[0]])
                            file_reporte.write('En el archivo CTG hubo cambio de %s por %s \n'
                                               % (brn_ctg[0], brns_dict[brn_ctg[0]]))
                        elif '&' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "&"]
                            gen_ctg.append(a_line[indices[0] + 1:indices[1]])
                            a_line = a_line.replace('&%s&' % gen_ctg[0], gen_load_dict[gen_ctg[0]])
                            file_reporte.write('En el archivo CTG hubo cambio de %s por %s \n'
                                               % (gen_ctg[0], gen_load_dict[gen_ctg[0]]))
                        elif '@' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "@"]
                            fx_ctg.append(a_line[indices[0] + 1:indices[1]])
                            if fx_ctg[0] in fxs_dict:
                                a_line = a_line.replace('&%s&' % fx_ctg[0], fxs_dict[fx_ctg[0]])
                                file_reporte.write('En el archivo CTG hubo cambio de %s por %s \n'
                                               % (fx_ctg[0], fxs_dict[fx_ctg[0]]))
                            else:
                                file_reporte.write('En el archivo CTG NO hubo cambio de %s \n'
                                               % fx_ctg[0])
                                continue
                        contingencia.append(a_line)
                    elif val == 'ITF':
                        brn_itf = []
                        if '#' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "#"]
                            brn_itf.append(a_line[indices[0] + 1:indices[1]])
                            a_line = a_line.replace('#%s#' % brn_itf[0], brns_dict[brn_itf[0]])
                            file_reporte.write('En el archivo ITF hubo cambio de %s por %s \n'
                                               % (brn_itf[0], brns_dict[brn_itf[0]]))
                        interface.append(a_line)
                    elif val == 'TRF':
                        gen_trf = []
                        if '&' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "&"]
                            gen_trf.append(a_line[indices[0] + 1:indices[1]])
                            if gen_trf[0] in gen_load_dict:
                                a_line = a_line.replace('&%s&' % gen_trf[0], gen_load_dict[gen_trf[0]])
                                file_reporte.write('En el archivo TRF hubo cambio de %s por %s \n'
                                                    % (gen_trf[0], gen_load_dict[gen_trf[0]]))
                            else:
                                file_reporte.write('NO SE ENCONTRO LA CLAVE %s \n' % gen_trf[0])
                                continue
                        elif 'Step' in a_line[0:4]:
                            a_line = a_line % cfg['STEP_SIZE']
                        elif 'Cutoff' in a_line:
                            a_line = a_line % cfg['CUTOFF_STEP_SIZE']
                        transferencia.append(a_line)
                    elif val == 'SPS':
                        buses_sps = []
                        gen_sps = []
                        brn_sps = []
                        if '$' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "$"]
                            for j in range(0, len(indices), 2):
                                k = j + 1
                                buses_sps.append(a_line[indices[j] + 1:indices[k]])
                            for m in range(len(buses_sps)):
                                a_line = a_line.replace('$%s$' % buses_sps[m], ', '.join(bus_dict[buses_sps[m]]))
                                file_reporte.write('En el archivo SPS hubo cambio de %s por %s \n'
                                                   % (buses_sps[m], ', '.join(bus_dict[buses_sps[m]])))
                        elif '&' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "&"]
                            gen_sps.append(a_line[indices[0] + 1:indices[1]])
                            a_line = a_line.replace('&%s&' % gen_sps[0], gen_load_dict[gen_sps[0]])
                            file_reporte.write('En el archivo SPS hubo cambio de %s por %s \n'
                                               % (gen_sps[0], gen_load_dict[gen_sps[0]]))
                        elif '#' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "#"]
                            brn_sps.append(a_line[indices[0] + 1:indices[1]])
                            a_line = a_line.replace('#%s#' % brn_sps[0], brns_dict[brn_sps[0]])
                            file_reporte.write('En el archivo SPS hubo cambio de %s por %s \n'
                                               % (brn_sps[0], brns_dict[brn_sps[0]]))
                        sps.append(a_line)
                    elif val == 'RMC':
                        if 'Control generator voltage settings' in a_line:
                            a_line = a_line % cfg['CONTROL_GEN_VOL']
                        elif 'Control SVC/shunt voltage settings' in a_line:
                            a_line = a_line % cfg['CONTROL_SVC_VOL']
                        elif 'Control switchable shunts' in a_line:
                            a_line = a_line % cfg['CONTROL_SW_SHUNTS']
                        elif 'Control transformer tap settings' in a_line:
                            a_line = a_line % cfg['CONTROL_TRANS_TAP']
                        elif 'Control load shedding' in a_line:
                            a_line = a_line % cfg['CONTROL_LOAD_SHED']
                        elif 'Control generation dispatch' in a_line:
                            a_line = a_line % cfg['CONTROL_GEN_DISPATCH']
                        remedial_control.append(a_line)

            if '/' in dict_compuerta['ENLACE']:
                name = dict_compuerta['ENLACE'].replace('/', '-')
            else:
                name = dict_compuerta['ENLACE']

            file_snr = open(os.path.join(path_fecha,'%s_%s.snr')%(name, flow), "w")
            file_prm = open(os.path.join(path_fecha,'%s_%s.prm')%(name, flow), "w")
            file_crt = open(os.path.join(path_fecha,'%s_%s.crt')%(name, flow), "w")
            file_mon = open(os.path.join(path_fecha,'%s_%s.mon')%(name, flow), "w")
            file_ctg = open(os.path.join(path_fecha,'%s_%s.ctg')%(name, flow), "w")
            file_itf = open(os.path.join(path_fecha,'%s_%s.itf')%(name, flow), "w")
            file_trf = open(os.path.join(path_fecha,'%s_%s.trf')%(name, flow), "w")
            
            file_snr.writelines(["%s\n" % item for item in scenario])
            file_prm.writelines(["%s\n" % item for item in parameter])
            file_crt.writelines(["%s\n" % item for item in criteria])
            file_mon.writelines(["%s\n" % item for item in monitoreo])
            file_ctg.writelines(["%s\n" % item for item in contingencia])
            file_itf.writelines(["%s\n" % item for item in interface])
            file_trf.writelines(["%s\n" % item for item in transferencia])
            
            file_snr.close()
            file_prm.close()
            file_crt.close()
            file_mon.close()
            file_ctg.close()
            file_itf.close()
            file_trf.close()

            if 'SPS' in dict_compuerta:
                file_sps = open(os.path.join(path_fecha,'%s_%s.sps')%(name, flow), "w")
                file_sps.writelines(["%s\n" % item for item in sps])
                file_sps.close()
            if 'RMC' in dict_compuerta:
                file_rmc = open(os.path.join(path_fecha,'%s_%s.rmc')%(name, flow), "w")
                file_rmc.writelines(["%s\n" % item for item in remedial_control])
                file_rmc.close()

    file_curvas = open(os.path.join(path_fecha, 'CURVAS_CAPABILIDAD.gcc'), "w")
    file_curvas.writelines(["%s\n" % item for item in new_curvas])
    file_curvas.close()
    file_reporte.close()


def run_vsat(path_fecha, path_data, PATHVSATBATCH, log_sys='SIN'):
    """
    Ejecuta analisis de estabilidad de voltaje
    """
    log_extra = {'sistema': log_sys}
    log.info('Ejecutando VSAT para estudio de cargabilidad', extra = log_extra)
    
    os.chdir(path_fecha)
    cmd = PATHVSATBATCH + ' < vsat_batch.dat'
    pVSAT = subprocess.Popen(cmd, shell=True)
    pVSAT.communicate()
    os.chdir(path_data)

    return 0


def save_lim(f_raw, lst_paths, sys_num=0):
    """
    """
    # create logger
    system = {0:'SIN', 1:'BCA', 2:'BCS'}
    sys_str = system[sys_num]
    log_extra = {'sistema': sys_str}
    log.info('Generando reporte de limites en compuertas', extra=log_extra)
    fecha_raw = get_datetime_raw(f_raw, sistema=sys_num, rpl_tz=False, log_sys=sys_str)

    date = datetime.today()
    date = date.strftime("%d-%m-%Y_%H-%M")
    folder = f_raw.split('\\')[-1]

    ###Clasifica a los paths en distintos diccionarios dependiendo la causa de paro de la PV
    termico = {}
    voltaje = {}
    max_min = {}

    ####Diccionario con los limites de transmision####
    lim_trans = {}
    inc = 0
    elemento = ' '
    for p in lst_paths:
        path = open(f_raw + '\\' + p +'.lmt','r')
        path_conts = path.readlines()
        inc = None
        elemento = None

        for j, enl in enumerate(path_conts):
            enl = enl.replace('\n','')
            if j == 6:
                causa = enl.split(',')[-1]  ##Extrae la causa de paro de la curva PV
                elemento = ((enl.split(',')[3]).lstrip()).rstrip()    ##Quita los espacios de la izquierda y de la derecha de la cadena
            
            ###Cuando el caso base es inseguro y al aplicar contingencia se llega al colapso
            if j == 5:
                causa_vc_icb = enl.split(',')[-1]
                if 'Insecure Base' in causa_vc_icb:
                    icb_vc = causa_vc_icb
                else:
                    icb_vc = ' '
                            
            if 'Interface Name' in enl:             ##Ubica en que renglon se debe leer el limite de transmisón
                inc = j+2
            
            try:
                if j == inc:
                    if 'Reached' in causa:
                        reach_limit = float(enl.split(',')[-1])
                        max_min[p] = [reach_limit,causa.rstrip()]
                        lim_trans[p] = [reach_limit,1]
         
                    if 'Thermal' in causa and 'Pre-contg' not in elemento and 'Insecure Base' not in icb_vc:
                        reach_limit = float(enl.split(',')[-3])
                        termico[p] = [reach_limit,elemento,'-']
                        lim_trans[p] = [reach_limit,2]

                    if 'Thermal' in causa and 'Pre-contg' in elemento:# and 'Insecure Base' not in icb_vc:
                        pass

                    if 'Thermal' in causa and 'Insecure Base' in icb_vc:
                        reach_limit = float(enl.split(',')[-4])
                        termico[p] = [reach_limit,elemento, icb_vc]
                        lim_trans[p] = [reach_limit,3]

                    if 'Voltage Violation' in causa and 'Pre-contg' not in elemento and 'Insecure Base' not in icb_vc:
                        reach_limit = float(enl.split(',')[-3])
                        voltaje[p] = [reach_limit,elemento,icb_vc,'-']
                        lim_trans[p] = [reach_limit,4]

                    if 'Voltage Collapse' in causa and 'Pre-contg' not in elemento and 'Insecure Base' not in icb_vc:
                        reach_limit = float(enl.split(',')[-2])
                        voltaje[p] = [reach_limit,elemento,causa.rstrip(),'-']
                        lim_trans[p] = [reach_limit,5]
                    
                    if 'Voltage Collapse' in causa and 'Insecure Base' in icb_vc:        
                        reach_limit =  float(enl.split(',')[-4]) 
                        voltaje[p] = [reach_limit,elemento,causa.rstrip(),icb_vc]
                        lim_trans[p] = [reach_limit,6]

                    if 'Voltage Violation' in causa and 'Pre-contg' in elemento and 'Insecure Base' not in icb_vc:
                        reach_limit = float(enl.split(',')[-4])
                        voltaje[p] = [reach_limit,elemento,icb_vc,'-']
                        lim_trans[p] = [reach_limit,7]
                    if 'Voltage Violation' in causa and 'Pre-contg' not in elemento and 'Insecure Base' in icb_vc:
                        reach_limit = float(enl.split(',')[-4])
                        voltaje[p] = [reach_limit,'Several Voltage Violations','-',icb_vc]
                        lim_trans[p] = [reach_limit,8]              
            except:
                pass

    ####Lectura de archivos de sobrecargas### 
    pos_ovl = 0
    for k, v in termico.iteritems():
        path = open(f_raw + '\\' + k +'-ovl.rpt','r')
        ovl_path = path.readlines()
        del pos_ovl
        
        for i, linea in enumerate(ovl_path):
            linea = linea.replace('\n','')
            if v[1] in linea:
                pos_ovl = i+5
            try:
                if i == pos_ovl:
                    cadena = (((re.sub(' +',' ',linea))).lstrip()).split()
                    nbus1 = cadena[0]
                    name1 = cadena[1]
                    nbus2 = cadena[3]
                    name2 = cadena[4]
                    lim_viol = float(cadena[-2])
                    violacion = float(cadena[-3])
                    dif_lim_viol = violacion-lim_viol 
                    rama_viol = str(nbus1+' '+name1+' '+nbus2+' '+name2)
                    termico[k] = [v[0],v[1],v[2],rama_viol,lim_viol,dif_lim_viol]
            except:
                pass

    ####Lectura de violaciones de voltaje#####
    pos_vlt = 0
    for k,v in voltaje.iteritems():
        if 'Voltage Collapse' not in v[1]:
            path = open(f_raw + '\\' + k +'-vlt.rpt','r')
            vlt_path = path.readlines()

            try:
                del pos_vlt
            except:
                pass
        
            for i, linea in enumerate(vlt_path):
                linea = linea.replace('\n','')
                if v[1] in linea:
                    pos_vlt = i+5
                try:
                    if i == pos_vlt:
                        cadena = (((re.sub(' +',' ',linea))).lstrip()).split()
                        n_bus = cadena[0]
                        bus_nam = cadena[1]
                        vlv = float(cadena[5]) 
                        nodo_viol = str(n_bus+' '+bus_nam)
                        voltaje[k] = [v[0],v[1],v[2],v[3],nodo_viol,vlv]
                except:
                    pass

    #######Escribe los archivos de salida, reporte de limites y archivos de reportes de violaciones#####
    lim_file = os.path.join(r'C:\VSAT\resumen', sys_str, 'Limites', 'limites_vsat_'+folder+'.csv')
    file_lim =  open(lim_file,'w')
    file_lim.write('ENLACE'+','+'LIMITE'+'\n')

    lim_fg = []
    for k, v in lim_trans.iteritems():
        file_lim.write(str(k) + ',' + str(v) +'\n')          ###Archivo csv de limites de transmision
        lim_fg.append({'ENL':str(k), 'LIM':v[0], 'CRT':v[1]})

    cl, db = get_Mongo_db('VSAT', sis=sys_str)
    coll = db.LIMTR
    index = coll.create_index([('FECHA', pymongo.ASCENDING)], unique=True)
    try:
        res = coll.insert_one({'FECHA': fecha_raw,
                               'SISTEMA': sys_str,
                               'SOFTWARE': 'DSATOOLS',
                               'FLOWGATE': lim_fg})
    except pymongo.errors.DuplicateKeyError, e:
        log.warning('%s'%e, extra=log_extra)

    cl.close()

    try:
        log_file = os.path.join(r'C:\VSAT\resumen', sys_str, 'Logger', 'logger_'+folder+'.csv')
        file_log = open(log_file,'w')
        file_log.write('COMPUERTA'+','+'LIMITE'+','+'PEOR CONTINGENCIA'+','+'BUS VIOLADO'+','+'VOLTAJE DE VIOLACION'+','+'CASO'+'\n')
        for k, v in voltaje.iteritems():
            #print k, v
            if 'Insecure Base' in v[3] and 'Several' in v[1]:
                file_log.write(str(k)+ ',' +str(v[0])+','+'-'+','+str(v[1])+','+'-'+','+str(v[3])+'\n')

            if 'Insecure' in v[3] and 'Collapse' in v[2]:
                file_log.write(str(k)+ ',' +str(v[0])+','+str(v[1])+','+'-'+','+str(v[2])+','+str(v[3])+'\n')        

            if 'Voltage Collapse' in v[2] and '-' == v[3]:
                file_log.write(str(k)+ ',' +str(v[0])+','+str(v[1])+','+'-'+','+str(v[2])+','+'-'+'\n')

            if 'Several' not in v[1] and 'Collapse' not in v[2]:
                file_log.write(str(k)+ ',' +str(v[0])+','+str(v[1])+','+str(v[4])+','+str(v[5])+','+'-'+'\n')

        file_log.write('\n'+'COMPUERTA'+','+'LIMITE'+','+'PEOR CONTINGENCIA'+','+'RAMA VIOLADA'+','+'LIMITE VIOLADO EN MVA'+','+'VIOLACION EN MVA'+','+'CASO'+'\n')
        for k, v in termico.iteritems():
            file_log.write(str(k)+ ',' +str(v[0])+','+str(v[1])+','+str(v[3])+','+str(v[4])+','+str(v[5])+','+str(v[2])+'\n')

        file_log.write('\n'+'COMPUERTA'+','+'LIMITE'+','+'CAUSA DE PARO'+'\n')
        for k, v in max_min.iteritems():
            file_log.write(str(k)+ ',' +str(v[0])+','+str(v[1])+'\n')

        ####Escribe historico individual de limites de compuerta####
        for enlace in lst_paths:
            his_file = os.path.join(r'C:\VSAT\resumen', sys_str, 'Historico',  enlace+'_hist.csv')
            with open(his_file,'a') as f:
                for k, v in lim_trans.iteritems():
                    if enlace == k:
                        f.write(str(folder[0:9])+','+str(folder[10:12]+'-'+folder[12:])+','+str(v[0])+','+str(v[1])+'\n') 

        file_lim.close()
        file_log.close()
    except Exception as e:
        log.warning('Error: %s'%str(e), extra = log_extra)


def read_enl_mdb(sistema='SIN'):
    """ 
    Read from MongoDB base de datos VSAT
    """
    log_extra = {'sistema': sistema}
    log.info('==> Lectura de informacion de compuertas', extra = log_extra)
    cl, db = get_Mongo_db('VSAT', sis=sistema)
    coll = db.ENLACES

    cursor = coll.find({'SISTEMA': sistema})
    data = list(cursor)

    cl.close()

    return data


def get_Mongo_db(nombre, sis='SIN'):
    """
    Regresa una conexión a la base de datos VSAT
    """
    log_extra = {'sistema': sis}
    log.info('==> Realizando conexion a MongoDB', extra = log_extra)

    param = pyCRED.psw()

    cl = MongoClient(','.join(map(lambda x: '%s'%x, param.MONGO_DB_RS['servers'])),
                     replicaSet=param.MONGO_DB_RS['name'])
    cl.admin.authenticate(param.MONGO_DB['usr'], param.MONGO_DB['pwd'])

    return cl, cl[nombre]


def date2string(fecha):
    """
    Convierte fecha en string en formato: 06Jul2018
    """
    locale.setlocale(locale.LC_ALL, 'esp_esp')
    dia = fecha.strftime('%d%b%Y_%H%M%S')
    locale.setlocale(locale.LC_ALL, 'english_us')
    dia = list(dia)
    dia[2] = dia[2].upper()
    fecha_s = ''.join(dia)   

    return fecha_s



if __name__ == '__main__':
    pass