import pymysql

#####Local
# host = "localhost"
# usuario = ""
# password = ""

###aws
host ="database-dt.cljvdwbzjql5.eu-west-2.rds.amazonaws.com"
usuario = ""
password = ""

db_name = ""
port = 3306


db_connect = pymysql.connect(host = host,
                            user=usuario,
                            password=password,
                            port = port,
                            database = db_name,
                            cursorclass=pymysql.cursors.DictCursor)#con esta propiedad la bd devuelve diccionarios


def getHoteles():
    sql='''Select * from hoteles'''
    cursor=db_connect.cursor()
    cursor.execute(sql)
    resp=cursor.fetchall()
    cursor.close()
    return getEagerHoteles(resp)

def getHotelesDestacados():
    sql='''Select * from hoteles where destacado =True'''
    cursor=db_connect.cursor()
    cursor.execute(sql)
    resp=cursor.fetchall()
    
    for r in resp:
        lat_log=r['lat_long'].split(',')
        r['lat'] = float(lat_log[0])
        r['long'] = float(lat_log[1])
        r['servicios']=getServicios(r['id'])
        r['fotos'] =getFotos(r['id'])
    
    cursor.close()
    return resp

def getDestinos():
    sql='''(Select distinct ciudad as destino from hoteles)
            union
            (Select distinct pais as destino from hoteles)
       '''
    
    cursor =db_connect.cursor()
    cursor.execute(sql)
    resp= cursor.fetchall()
    cursor.close()

    return resp

def getHotelesFiltro(destino,fecha_ini,fecha_fin,personas,habitaciones):
    sql ='''select distinct hab.id
        from habitaciones as hab inner join reservas as r on hab.id=r.id_habitacion
        where (r.fecha_ini between STR_TO_DATE(%s, "%%d-%%m-%%Y") and STR_TO_DATE(%s, "%%d-%%m-%%Y")) or 
        (r.fecha_fin between STR_TO_DATE(%s, "%%d-%%m-%%Y") and STR_TO_DATE(%s, "%%d-%%m-%%Y")) or 
        (STR_TO_DATE(%s, "%%d-%%m-%%Y") between r.fecha_ini and r.fecha_fin) or 
        (STR_TO_DATE(%s, "%%d-%%m-%%Y") between r.fecha_ini and r.fecha_fin)'''
    
    params = [fecha_ini,fecha_fin,fecha_ini,fecha_fin,fecha_ini,fecha_fin]
    
    cursor=db_connect.cursor()
    cursor.execute(sql,params)
    resp=cursor.fetchall()

    idHabReservadas=[]
    for r in resp:
        idHabReservadas.append(r['id'])

    sql='''Select h.*
        from hoteles as h inner join habitaciones as hab on h.id=hab.id_hotel
        inner join (select distinct id
        from habitaciones
        where id not in %s) as qR
        on hab.id = qR.id '''
        # where (ciudad = 'España' or pais = 'España')
        # group by h.id
        # having sum(hab.personas) >=3 and count(hab.id)>=2;'''

    params = [idHabReservadas]

    if destino.strip() != '':
        sql += ' where (ciudad = %s or pais = %s) '
        params.extend([destino,destino])

    sql += ' group by h.id having sum(hab.personas) >=%s and count(hab.id)>=%s;'
    params.extend([personas,habitaciones])

    # params = tuple(params)
    
    cursor.execute(sql,params)
    resp=cursor.fetchall()
    
    
    for r in resp:
        lat_log=r['lat_long'].split(',')
        r['lat'] = float(lat_log[0])
        r['long'] = float(lat_log[1])
        r['servicios']=getServicios(r['id'])
        r['fotos']=getFotos(r['id'])
        respHab = getHabitacionesFiltro(r['id'],idHabReservadas)
        precioMin=1000000
        idHabDisponibles =[]
        for h in respHab:
            idHabDisponibles.append(h['id_habitacion'])
            if precioMin > h['precio']:
                precioMin = h['precio']
            # h['reservas']=getReserva(h['id_habitacion'])
        #     h['equipamiento']=getEquipamiento(h['id'])
        
        r['habs_disponibles']=idHabDisponibles
        r['precio_minimo']=precioMin
        # r['habitaciones_disponibles']=respHab
    cursor.close()
    return resp

def getHotel(idHotel):
    sql ='''Select * from hoteles where id =%s'''
    cursor=db_connect.cursor()
    cursor.execute(sql,(idHotel,))
    resp=cursor.fetchone()
    cursor.close()
    return resp
    
def getServicios(idHotel):
    sql='''Select s.id as id_servicio,s.servicio, hs.favorito
     from hoteles as h inner join hotel_servicio as hs on h.id=hs.id_hotel
     inner join servicios as s on hs.id_servicio=s.id
     where h.id = %s'''
    cursor=db_connect.cursor()
    cursor.execute(sql,(idHotel,))
    resp=cursor.fetchall()
    cursor.close()
    return resp

def getFotos(idHotel):
    sql='''Select f.id as id_foto,f.path
     from hoteles as h inner join fotos as f on h.id=f.id_hotel
     where h.id = %s'''
    cursor=db_connect.cursor()
    cursor.execute(sql,(idHotel,))
    resp=cursor.fetchall()
    cursor.close()
    return resp

def getHabitaciones(idHotel):
    sql='''Select hab.id as id_habitacion, hab.tipo, hab.precio,hab.personas
     from hoteles as h inner join habitaciones as hab on h.id=hab.id_hotel
     where h.id = %s'''
    cursor=db_connect.cursor()
    cursor.execute(sql,(idHotel,))
    resp=cursor.fetchall()
    cursor.close()
    return resp

def getHabitacionesFiltro(idHotel,idHabReservadas):
    sql='''Select hab.id as id_habitacion, hab.tipo, hab.precio,hab.personas
     from hoteles as h inner join habitaciones as hab on h.id=hab.id_hotel
     where h.id = %s and hab.id not in %s'''
    cursor=db_connect.cursor()
    cursor.execute(sql,(idHotel,idHabReservadas))
    resp=cursor.fetchall()

    return resp

def getHotelFiltro(idHotel,idHabDisponibles):
    resp = getHotel(idHotel)
    # for r in resp:
    lat_log=resp['lat_long'].split(',')
    resp['lat'] = float(lat_log[0])
    resp['long'] = float(lat_log[1])
    resp['servicios'] = getServicios(idHotel)
    resp['habitaciones'] = getHabitacionesDisponibles(idHotel,idHabDisponibles)
    resp['fotos'] =getFotos(idHotel)
    return resp

def getHabitacionesDisponibles(idHotel,idHabDisponibles):
    sql='''Select hab.id as id_habitacion, hab.tipo, hab.precio,hab.personas
     from hoteles as h inner join habitaciones as hab on h.id=hab.id_hotel
     where h.id = %s and hab.id in %s'''
    cursor=db_connect.cursor()
    cursor.execute(sql,(idHotel,idHabDisponibles))
    resp=cursor.fetchall()

    return resp

def getReserva(idHabitacion):
    sql='''Select r.id as id_reserva,DATE_FORMAT(r.fecha_ini, "%%d-%%m-%%Y") as fecha_ini,
    DATE_FORMAT(r.fecha_fin, "%%d-%%m-%%Y") as fecha_fin
     from habitaciones as h inner join reservas as r on h.id=r.id_habitacion
     where h.id = %s'''
    cursor=db_connect.cursor()
    cursor.execute(sql,(idHabitacion,))
    resp=cursor.fetchall()
    cursor.close()
    return resp

def getEquipamiento(idHabitacion):
    sql='''Select e.*
     from habitaciones as h inner join habitacion_equipamiento as he on h.id=he.id_habitacion
     inner join equipamiento as e on he.id_equipamiento=e.id
     where h.id = %s'''
    cursor=db_connect.cursor()
    cursor.execute(sql,(idHabitacion,))
    resp=cursor.fetchall()
    cursor.close()
    return resp

def getEagerHoteles(respHotel):
    resp=respHotel.copy()
    for r in resp:
        r['servicios']=getServicios(r['id'])
        respHab = getHabitaciones(r['id'])
        for h in respHab:
             h['reservas']=getReserva(h['id_habitacion'])
        #     h['equipamiento']=getEquipamiento(h['id'])
        
        r['habitaciones']=respHab
    
    return resp
