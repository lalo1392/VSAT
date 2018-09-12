import sys
import os
import glob
import tipo_archivo
import numero_buses
import logging
import logging.handlers
from time import gmtime, strftime
import numpy as np
import time
import mongodb_enlaces


def Ruta_actual():                          # Obtener la ruta actual donde se ubica este archivo
    pathname = os.path.dirname(sys.argv[0])
    path_act = os.path.abspath(pathname)
    path_act = path_act.rstrip(' ')
    return path_act


def ls(ruta='.'):                           # Obtener la lista de archivos en el directorio actual
    return next(os.walk(ruta))[2]


def find_label(filename, label1, label2, label3, label4, label5, label6, label7, label8):
    indice1 = 0
    indice2 = 0
    indice3 = 0
    indice4 = 0
    indice5 = 0
    indice6 = 0
    indice7 = 0
    indice8 = 0
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
            elif label7 in line:
                indice7 = num
            elif label8 in line:
                indice8 = num
                break
    return indice1, indice2, indice3, indice4, indice5, indice6, indice7, indice8


def Leer_archivo(archivo, raw_ref, raw_nuevo):
    extensiones = ['.crt', '.mon', '.trf', '.itf', '.ctg']      # Solo las extensiones que si contienen nodos
    fecha = strftime("%d-%b-%Y %H_%M_%S", gmtime())

    for ext in extensiones:                                     # Leer los archivos de VSAT a mapear
        if ext == '.crt':
            tipo_archivo.archivo_crt(archivo, raw_ref, raw_nuevo, fecha, ext)
        elif ext == '.mon':
            tipo_archivo.archivo_mon(archivo, raw_ref, raw_nuevo, fecha, ext)
        if ext == '.trf':
            tipo_archivo.archivo_trf(archivo, raw_ref, raw_nuevo, fecha, ext)
        elif ext == '.itf':
            tipo_archivo.archivo_itf(archivo, raw_ref, raw_nuevo, fecha, ext)
        elif ext == '.ctg':
            tipo_archivo.archivo_ctg(archivo, raw_ref, raw_nuevo, fecha, ext)


def Elimina_linea(archivo_psse, ruta):
    file = open(archivo_psse, "r")
    ruta_lista = archivo_psse.split('\\')
    archivo_psse_nuevo = ruta + '\\Archivos de Salida\\New_' + ruta_lista[-1]
    raw = file.read()
    raw_filas = np.array(raw.split('\n'))

    # ETIQUETAS
    lab_bus = '0 / END OF BUS DATA, BEGIN LOAD DATA'
    lab_load = '0 / END OF LOAD DATA, BEGIN FIXED SHUNT DATA'
    lab_genI = '0 / END OF FIXED SHUNT DATA, BEGIN GENERATOR DATA'
    lab_genF = '0 / END OF GENERATOR DATA, BEGIN BRANCH DATA'
    lab_branch = '0 / END OF BRANCH DATA, BEGIN TRANSFORMER DATA'
    lab_area = '0 / END OF TRANSFORMER DATA, BEGIN AREA DATA'
    lab_swi = '0 / END OF FACTS DEVICE DATA, BEGIN SWITCHED SHUNT DATA'
    lab_swe = '0 / END OF SWITCHED SHUNT DATA, BEGIN GNE DEVICE DATA'
    lab_dc = '0 / END OF AREA DATA, BEGIN TWO-TERMINAL DC DATA'

    bus_end, load_end, gen_ini, gen_end, brch_end, area_ini, sw_ini, sw_end = find_label(archivo_psse, lab_bus, lab_load, lab_genI,
                                                                         lab_genF, lab_branch, lab_area, lab_swi, lab_swe)

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

    # Lee Switched Shunts Data
    sw_shunt_raw = np.array(raw_filas[sw_ini:sw_end-1])

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
            print 'Linea eliminada:', lineas_raw[j]
            lineas_raw = np.delete(lineas_raw, j, None)
        elif n_env in nodos_env:
            for idx, item in enumerate(nodos_env):
                if item == n_env:
                    pos_e.append(idx)
            for m in pos_e:
                if n_rec == nodos_rec[m]:
                    if iden == IDs[m]:
                        if impedancia <= z_ramas[m]:
                            print 'Linea eliminada:', lineas_raw[j]
                            lineas_raw = np.delete(lineas_raw, j, None)
                        else:
                            print 'Linea eliminada:', lineas_raw[len(lineas_raw)-m-1]
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
                            print 'Linea eliminada:', lineas_raw[j]
                            lineas_raw = np.delete(lineas_raw, j, None)
                        else:
                            print 'Linea eliminada:', lineas_raw[len(lineas_raw)-n-1]
                            # del(lineas_raw[len(lineas_raw)-n-1])
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
    file_new = open(archivo_psse_nuevo, "w")

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
    # raw_nuevo = np.append(raw_nuevo, lab_dc)
    # raw_nuevo = np.append(raw_nuevo, end_raw)

    file_new.writelines("%s\n" % item for item in raw_nuevo)
    file.close()
    file_new.close()
    return archivo_psse_nuevo, buses_raw, cargas_raw, generadores_raw, lineas_raw, trafos_raw, fixed_shunt_raw, sw_shunt_raw


def programa_principal(raw_entrada):
    ini = time.time()

    exclude_kvs = '230'
    high_limit = '1.05'
    low_limit = '0.95'
    s_size = '50'
    cs_size = '1'
    enable_cc = 'T'

    # ELIMINAR LAS LINEAS REPETIDAS DEL RAW DE ENTRADA
    ruta = Ruta_actual()
    raw_nuevo, buses_raw, cargas_raw, generadores_raw, lineas_raw, trafos_raw, fixed_shunt_raw, sw_shunt_raw = \
        Elimina_linea(raw_entrada, ruta)
    archivo_pfb = '.'.join(raw_nuevo.split('.')[0:-1]) + '.pfb'

    ##########################################################################################
    b_ramas = []            # Lista con los buses que aparecen en la seccion de ramas
    for b in lineas_raw:
        linea = b.split(',')
        b_ramas.append(linea[0].strip())
        b_ramas.append(linea[1].strip())

    b_buscar = set(b_ramas)     # Buses no repetidos
    n_rep = []                    # Numeros de veces que aparece cada bus en la seccion de ramas
    for s in b_buscar:
        n_rep.append((s, b_ramas.count(s)))
    #############################################################################################

    # GENERAR LOS DICCIONARIOS A PARTIR DEL RAW QUE LLEGUE DEL ESTIMADOR
    bus_dict = {}
    bus_dict_aux = {}
    bus_dict_com = {}                   # **************************
    gen_load_dict = {}
    brns_dict = {}
    fxs_dict = {}
    sws_dict = {}
    for l, line in enumerate(buses_raw):    # DICCIONARIO 1 (BUSES)
        datos = line.split(',')
        kvs = datos[2].strip()
        area = datos[4].strip()
        zona_c = datos[10].strip()
        siglas = datos[1][1:4]
        edo = datos[3].strip()
        if '.00' in kvs:
            kvs = str(int(float(kvs)))
        else:
            kvs = str(round(float(kvs), 2))
        if int(datos[4].strip()) < 10:
            area = '0' + area
        clave = zona_c + siglas + '-' + kvs
        bus_dict_aux[datos[0].strip()] = clave
        if edo == '4':
            try:
                bus_dict_com[clave].append(datos[0].strip())
            except:
                bus_dict_com[clave] = [datos[0].strip()]
            continue
        if clave in bus_dict:
            bus_dict[clave].append(datos[0].strip())
            bus_dict_com[clave].append(datos[0].strip())
        else:
            bus_dict[clave] = [datos[0].strip()]
            bus_dict_com[clave] = [datos[0].strip()]
    # print mydict.keys()[mydict.values().index(16)] find key from value

    #########################################################################
    cont_buses = {}     # Diccionario formato (N_BUS: n_rep)
    for n in n_rep:
        try:
            cont_buses[n[0]] = n[1]
        except:
            continue
    #########################################################################

    for l, line in enumerate(cargas_raw):   # DICCIONARIO 2 (CARGAS Y GENERADORES)
        datos = line.split(',')
        num = datos[0].strip()
        iden = datos[1]         # .strip().strip('\'').strip()
        try:
            clave = bus_dict_aux[num] + 'L' + iden.strip().strip('\'').strip()
            gen_load_dict[clave] = num + ' ' + iden
        except:
            continue
    for l, line in enumerate(generadores_raw):
        datos = line.split(',')
        num = datos[0].strip()
        iden = datos[1]         # .strip().strip('\'').strip()
        try:
            clave = bus_dict_aux[num] + 'G' + iden.strip().strip('\'').strip()
            gen_load_dict[clave] = num + ' ' + iden
        except:
            continue

    for l, line in enumerate(lineas_raw):   # DICCIONARIO 3 (RAMAS: LINEAS Y TRAFOS)
        datos = line.split(',')
        num_e = datos[0].strip()
        num_r = datos[1].strip()
        iden = datos[2]
        try:
            clave = bus_dict_aux[num_e] + ' ' + bus_dict_aux[num_r] + ' ' + iden
            brns_dict[clave] = num_e + ' ' + num_r + ' ' + iden
            clave_i = bus_dict_aux[num_r] + ' ' + bus_dict_aux[num_e] + ' ' + iden
            brns_dict[clave_i] = num_r + ' ' + num_e + ' ' + iden
        except:
            continue
    for l, line in enumerate(trafos_raw):
        if l == 0 or l % 4 == 0:
            datos = line.split(',')
            num_e = datos[0].strip()
            num_r = datos[1].strip()
            iden = datos[3]
            try:
                clave = bus_dict_aux[num_e] + ' ' + bus_dict_aux[num_r] + ' ' + iden
                brns_dict[clave] = num_e + ' ' + num_r + ' ' + iden
                clave_i = bus_dict_aux[num_r] + ' ' + bus_dict_aux[num_e] + ' ' + iden
                brns_dict[clave_i] = num_r + ' ' + num_e + ' ' + iden
            except:
                continue
    for l, line in enumerate(fixed_shunt_raw):  # DICCIONARIO 4
        datos = line.split(',')
        num = datos[0].strip()
        iden = datos[1]
        try:
            clave = bus_dict_aux[num] + 'F' + iden.strip().strip('\'').strip()
            fxs_dict[clave] = num + ' ' + iden
        except:
            continue
    for l, line in enumerate(sw_shunt_raw):  # DICCIONARIO 5
        datos = line.split(',')
        num = datos[0].strip()
        try:
            clave = bus_dict_aux[num] + 'SW'
            sws_dict[clave] = num
        except:
            continue

    # CREAR COPIAS DE LOS DICCIONARIOS PARA USARLOS INDEPENDIENTEMENTE
    # bus_dict2 = bus_dict.copy()
    # bus_dict_aux2 = bus_dict_aux.copy()
    # gen_load_dict2 = gen_load_dict.copy()
    # brns_dict2 = brns_dict.copy()
    # file_dicc = open(r'C:\DEPTO_APPS\Diccionarios\Nuevos\Diccionario Buses.txt', 'w')
    # file_dicc2 = open(r'C:\DEPTO_APPS\Diccionarios\Nuevos\Diccionario Generadores y Cargas.txt', 'w')
    # file_dicc3 = open(r'C:\DEPTO_APPS\Diccionarios\Nuevos\Diccionario Ramas.txt', 'w')
    # print '\nDICCIONARIO DE BUSES\n'
    # for (k, v) in bus_dict.items():
    #     buses = ', '.join(v)
    #     file_dicc.writelines(k+": ["+buses+"]\n")
    #     # print k, ":", v
    #
    # print '\nDICCIONARIO DE CARGAS Y GENERADORES\n'
    # for (k, v) in gen_load_dict.items():
    #     file_dicc2.writelines(k + ": [" + v + "]\n")
    #     # print k, ":", v
    #
    # print '\nDICCIONARIO DE LINEAS Y TRANSFORMADORES\n'
    # for (k, v) in brns_dict.items():
    #     file_dicc3.writelines(k + ": [" + v + "]\n")
    #     # print k, ":", v
    #
    # file_dicc.close()
    # file_dicc2.close()
    # file_dicc3.close()

    # LEER EL MACHOTE DE CADA COMPUERTA

    db_compuerta = mongodb_enlaces.main()
    dict_curvas = db_compuerta[90]
    curvas = dict_curvas['DATA']

    file_reporte = open(r'D:\O30164.CENACE\My Documents\VSAT_v4\Archivos de Salida\cambios.txt', 'w')
    for ite in range(len(db_compuerta)):
        scenario = []
        parameter = []
        criteria = []
        monitoreo = []
        contingencia = []
        interface = []
        transferencia = []
        tipo = ['SNR', 'PRM', 'CRT', 'MON', 'CTG', 'ITF', 'TRF']
        # db_compuerta = mongodb_enlaces.main()
        # dict_compuerta = read_bd()
        # if ite == len(db_compuerta)-1:
        # for c_line in curvas:
        #     if '&' in c_line:
        #         indices = [i for i, c in enumerate(c_line) if c == "&"]
        #         gen_gcc = c_line[indices[0] + 1:indices[1]]
        #         c_line = c_line.replace('&%s&' % gen_gcc, gen_load_dict[gen_gcc])
        #         file_reporte.write('En el archivo GCC hubo cambio de %s por %s \n'
        #                            % (gen_gcc, gen_load_dict[gen_gcc]))
        #     new_curvas.append(c_line)
        if db_compuerta[ite]['ENLACE'] == "CURVAS_CAPABILIDAD":
            new_curvas = []
            cn = 0  # OMITIR INFORMACION DE LOS GENERADORES NO ENCONTRADOS
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
            file_reporte.write(dict_compuerta['ENLACE'] + '\n')
            flow = dict_compuerta['SENTIDO']
            for k, val in enumerate(tipo):
                archivo = dict_compuerta[val]
                for a, a_line in enumerate(archivo[0]):
                    if val == 'SNR':
                        if 'Powerflow File' in a_line:
                            a_line = a_line % raw_nuevo
                        elif 'PFB File' in a_line:
                            a_line = a_line % archivo_pfb
                        scenario.append(a_line)
                    elif val == 'PRM':
                        if 'Exclude branches below kV level' in a_line:
                            a_line = a_line % exclude_kvs
                        elif 'Use generator capability curves' in a_line:
                            a_line = a_line % enable_cc
                        parameter.append(a_line)
                    elif val == 'CRT':
                        claves_crt = []
                        if '$' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "$"]
                            for j in range(0, len(indices), 2):
                                k = j + 1
                                claves_crt.append(a_line[indices[j] + 1:indices[k]])
                            # for m in range(len(claves_crt)):
                            #     if claves_crt[m] in bus_dict
                            #           a_line = a_line.replace('$%s$' % claves_crt[m], ', '.join(bus_dict[claves_crt[m]]))
                            #           file_reporte.write('En el archivo CRT hubo cambio de %s por %s \n'
                            #                        % (claves_crt[m], ', '.join(bus_dict[claves_crt[m]])))
                            #     else:
                            #           file_reporte.write('NO SE ENCONTRO LA CLAVE %s \n' % claves_crt[m])
                            #           continue
                            for m in range(len(claves_crt)):
                                try:
                                    if len(bus_dict[claves_crt[m]]) == 1:
                                        a_line = a_line.replace('$%s$' % claves_crt[m], ', '.join(bus_dict[claves_crt[m]]))
                                        file_reporte.write('En el archivo CRT hubo cambio de %s por %s \n'
                                                           % (claves_crt[m], ', '.join(bus_dict[claves_crt[m]])))
                                    else:
                                        buses_crt = bus_dict[claves_crt[m]]
                                        for p in range(len(buses_crt) - 1, -1, -1):
                                            # try:
                                            if cont_buses[buses_crt[-1]] > cont_buses[buses_crt[p-1]]:
                                                del (buses_crt[p-1])
                                            elif cont_buses[buses_crt[-1]] < cont_buses[buses_crt[p-1]]:
                                                del(buses_crt[p::])
                                        a_line = a_line.replace('$%s$' % claves_crt[m], ', '.join(buses_crt))
                                        file_reporte.write('En el archivo CRT hubo cambio de %s por %s \n'
                                                           % (claves_crt[m], ', '.join(buses_crt)))
                                except:
                                    continue
                        elif 'High' in a_line:
                            a_line = a_line % high_limit
                        elif 'Low' in a_line:
                            a_line = a_line % low_limit
                        criteria.append(a_line)
                    elif val == 'MON':
                        claves_mon = []
                        if '$' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "$"]
                            for j in range(0, len(indices), 2):
                                k = j + 1
                                claves_mon.append(a_line[indices[j] + 1:indices[k]])
                            # for m in range(len(claves_mon)):
                            #     a_line = a_line.replace('$%s$' % claves_mon[m], ', '.join(bus_dict[claves_mon[m]]))
                            #     file_reporte.write('En el archivo MON hubo cambio de %s por %s \n'
                            #                        % (claves_mon[m], ', '.join(bus_dict[claves_mon[m]])))
                            for m in range(len(claves_mon)):
                                try:
                                    if len(bus_dict[claves_mon[m]]) == 1:
                                        a_line = a_line.replace('$%s$' % claves_mon[m], ', '.join(bus_dict[claves_mon[m]]))
                                        file_reporte.write('En el archivo CRT hubo cambio de %s por %s \n'
                                                           % (claves_mon[m], ', '.join(bus_dict[claves_mon[m]])))
                                    else:
                                        buses_mon = bus_dict[claves_mon[m]]
                                        for p in range(len(buses_mon) - 1, -1, -1):
                                            # try:
                                            if cont_buses[buses_mon[-1]] > cont_buses[buses_mon[p-1]]:
                                                del(buses_mon[p-1])
                                            elif cont_buses[buses_mon[-1]] < cont_buses[buses_mon[p-1]]:
                                                del(buses_mon[p::])
                                        a_line = a_line.replace('$%s$' % claves_mon[m], ', '.join(buses_mon))
                                        file_reporte.write('En el archivo MON hubo cambio de %s por %s \n'
                                                           % (claves_mon[m], ', '.join(buses_mon)))
                                except:
                                    continue
                        monitoreo.append(a_line)
                    elif val == 'CTG':
                        buses_ctg = []
                        brn_ctg = []
                        gen_ctg = []
                        sw_ctg = []
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
                            # for j in range(0, len(indices), 2):
                            #     k = j + 1
                            brn_ctg.append(a_line[indices[0] + 1:indices[1]])
                            # for m in range(len(brn_ctg)):
                            a_line = a_line.replace('#%s#' % brn_ctg[0], brns_dict[brn_ctg[0]])
                            file_reporte.write('En el archivo CTG hubo cambio de %s por %s \n'
                                               % (brn_ctg[0], brns_dict[brn_ctg[0]]))
                        elif '&' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "&"]
                            # for j in range(0, len(indices), 2):
                            #     k = j + 1
                            gen_ctg.append(a_line[indices[0] + 1:indices[1]])
                            # for m in range(len(brn_ctg)):
                            a_line = a_line.replace('&%s&' % gen_ctg[0], gen_load_dict[gen_ctg[0]])
                            file_reporte.write('En el archivo CTG hubo cambio de %s por %s \n'
                                               % (gen_ctg[0], gen_load_dict[gen_ctg[0]]))
                        # contingencia.append(a_line)
                        elif '*' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "*"]
                            # for j in range(0, len(indices), 2):
                            #     k = j + 1
                            sw_ctg.append(a_line[indices[0] + 1:indices[1]])
                            # for m in range(len(brn_ctg)):
                            a_line = a_line.replace('*%s*' % sw_ctg[0], sws_dict[sw_ctg[0]])
                            file_reporte.write('En el archivo CTG hubo cambio de %s por %s \n'
                                               % (sw_ctg[0], sws_dict[sw_ctg[0]]))
                        contingencia.append(a_line)

                    elif val == 'ITF':
                        brn_itf = []
                        if '#' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "#"]
                            # for j in range(0, len(indices), 2):
                            #     k = j + 1
                            brn_itf.append(a_line[indices[0] + 1:indices[1]])
                            # for m in range(len(brn_ctg)):
                            a_line = a_line.replace('#%s#' % brn_itf[0], brns_dict[brn_itf[0]])
                            file_reporte.write('En el archivo ITF hubo cambio de %s por %s \n'
                                               % (brn_itf[0], brns_dict[brn_itf[0]]))
                        interface.append(a_line)
                    elif val == 'TRF':
                        gen_trf = []
                        if '&' in a_line:
                            indices = [i for i, c in enumerate(a_line) if c == "&"]
                            gen_trf.append(a_line[indices[0] + 1:indices[1]])
                            try:
                                a_line = a_line.replace('&%s&' % gen_trf[0], gen_load_dict[gen_trf[0]])
                                file_reporte.write('En el archivo TRF hubo cambio de %s por %s \n'
                                               % (gen_trf[0], gen_load_dict[gen_trf[0]]))
                            except:
                                continue
                        elif 'Step' in a_line[0:4]:
                            a_line = a_line % s_size
                        elif 'Cutoff' in a_line:
                            a_line = a_line % cs_size
                        transferencia.append(a_line)

            # for l in range(len(data_compuerta)):
            #     busquedas = data_compuerta[line].count('$')
            #     if busquedas > 0:
            #         print 'algo'

            # ruta_compuerta = r'C:\DEPTO_APPS\mongoDB_propuesta.txt'
            # file_compuerta = open(ruta_compuerta, 'r')
            # file_data = file_compuerta.read()
            # file_data = file_data.split('\n')
            # doc = file_compuerta.read()
            # lines = file_compuerta.read().split('\n')
            # lines = doc.split('\n')
            # elementos = doc.split(',')
            # file_compuerta.close()

            # diccionario = {}
            # for line in lines:
            #     if len(line.strip()) > 2:
            #         diccionario[line.split(":")[0]] = line.split(":")[1]

            if '/' in dict_compuerta['ENLACE']:
                name = dict_compuerta['ENLACE'].replace('/', '-')
            else:
                name = dict_compuerta['ENLACE']
            file_snr = open(r'D:\O30164.CENACE\My Documents\VSAT_v4\Archivos de Salida\%s_%s.snr' % (name, flow), "w")
            file_prm = open(r'D:\O30164.CENACE\My Documents\VSAT_v4\Archivos de Salida\%s_%s.prm' % (name, flow), "w")
            file_crt = open(r'D:\O30164.CENACE\My Documents\VSAT_v4\Archivos de Salida\%s_%s.crt' % (name, flow), "w")
            file_mon = open(r'D:\O30164.CENACE\My Documents\VSAT_v4\Archivos de Salida\%s_%s.mon' % (name, flow), "w")
            file_ctg = open(r'D:\O30164.CENACE\My Documents\VSAT_v4\Archivos de Salida\%s_%s.ctg' % (name, flow), "w")
            file_itf = open(r'D:\O30164.CENACE\My Documents\VSAT_v4\Archivos de Salida\%s_%s.itf' % (name, flow), "w")
            file_trf = open(r'D:\O30164.CENACE\My Documents\VSAT_v4\Archivos de Salida\%s_%s.trf' % (name, flow), "w")

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

    file_curvas = open(r'D:\O30164.CENACE\My Documents\VSAT_v4\Archivos de Salida\CURVAS_CAPABILIDAD.gcc', "w")
    file_curvas.writelines(["%s\n" % item for item in new_curvas])
    file_curvas.close()
    file_reporte.close()

    fin = time.time() - ini
    print fin

