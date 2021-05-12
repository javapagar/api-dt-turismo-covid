from  flask import Flask,jsonify,request
import apiService


app = Flask(__name__)

@app.route('/apidt/v1/test',methods=['GET'])
def test():
    return 'API para el desafío de tripulaciones'

@app.route('/apidt/v1/hoteles',methods=['GET'])
def getHoteles():
    resp =apiService.getHoteles()
    return jsonify(resp)

@app.route('/apidt/v1/hoteles/destacados',methods=['GET'])
def getHotelesDestacados():
    resp =apiService.getHotelesDestacados()
    return jsonify(resp)

@app.route('/apidt/v1/destinos',methods=['GET'])
def getDestinos():
    resp =apiService.getDestinos()
    return jsonify(resp)

@app.route('/apidt/v1/hoteles/filtro',methods=['GET'])
def getHotelesFiltrados():
    query_parameters = request.get_json()

    destino = query_parameters.get('destino')
    fecha_ini = query_parameters.get('fecha_ini')
    fecha_fin = query_parameters.get('fecha_fin')
    personas = query_parameters.get('personas')
    habitaciones=query_parameters.get('habitaciones')

    resp =apiService.getHotelesFiltro(destino,fecha_ini,fecha_fin,personas,habitaciones)
    return jsonify(resp)

@app.route('/apidt/v1/hotelfiltro',methods=['GET'])
def getHabitacionesFiltro():
    query_parameters = request.get_json()

    idHotel=query_parameters.get('id_hotel')
    habitaciones = query_parameters.get('habs_disponibles')

    resp =apiService.getHotelFiltro(idHotel,habitaciones)
    return jsonify(resp)



if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)