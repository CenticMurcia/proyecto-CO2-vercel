# Run the application (as long as is named app.py) with:
# $ flask run 
# $ python app.py 

import requests
from flask import Flask, render_template
from apscheduler.schedulers.background import BackgroundScheduler
#from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime
import pytz
import os


app = Flask(__name__)

hist_Hora = []
hist_CO2  = []
hist_PM10 = []
hist_PM25 = []
hist_Temperatura = []
hist_Humedad = []
hist_PersonasIn = []
hist_PersonasOut = []
hist_Personas = []

show_Hora = ["Iniciando..."]
show_CO2  = [-1]
show_PM10 = [-1]
show_PM25 = [-1]
CO2_msg   = ""
PM10_msg  = ""
PM25_msg  = ""

#################################### READ HOPU DATA

def get_datetime():
    # datetime object containing current date and time
    # dd/mm/YY H:M:S
    dt = datetime.now(pytz.timezone("Europe/Madrid"))
    date = dt.strftime("%d/%m/%Y")
    time = dt.strftime("%H:%M:%S")
    hora = dt.strftime("%H:%M")

    hist_Hora.append( hora )



# 1. Iniciar sesion en el APIRest de Hopu
#    Obtener access token y refress token

def API_get_token():
    url      = "https://fiware.hopu.eu/keycloak/auth/realms/fiware-server/protocol/openid-connect/token" 
    headers  = {"Content-Type": "application/x-www-form-urlencoded"}
    data     = "username=julgonzalez&password=vZnAWE7FexwgEqwT&grant_type=password&client_id=fiware-login"
    response = requests.post(url, data = data, headers = headers).json()

    global access_token, refresh_token

    access_token  = response["access_token"]
    refresh_token = response["refresh_token"]


def API_get_device_status(access_token):

    url      = "https://fiware.hopu.eu/orion/v2/entities?limit=1000&attrs=*,dateModified&options=count,keyValues" 
    headers  = {"fiware-service": "Device", "fiware-servicepath": "/ctcon", "Authorization": "Bearer "+access_token}
    response = requests.get(url, headers = headers).json()[0]

    global operationalStatus

    operationalStatus = response["operationalStatus"]



def API_get_calidad_aire(access_token):

    url      = "https://fiware.hopu.eu/orion/v2/entities?limit=1000&attrs=*,dateModified&options=keyValues" 
    headers  = {"fiware-service": "AirQuality", "fiware-servicepath": "/ctcon", "Authorization": "Bearer "+access_token}
    response = requests.get(url, headers = headers).json()[0]

    hist_CO2.append(         response["CO2"]         )
    hist_PM10.append(        response["PM10"]        )
    hist_PM25.append(        response["PM25"]        )
    hist_Temperatura.append( response["temperature"] )
    hist_Humedad.append(     response["humidity"]    )



def API_get_presencia(access_token):

    url      = "https://fiware.hopu.eu/orion/v2/entities?limit=1000&attrs=*,dateModified&options=count,keyValues" 
    headers  = {"fiware-service": "PeopleCounting", "fiware-servicepath": "/ctcon", "Authorization": "Bearer "+access_token}
    response = requests.get(url, headers = headers).json()[0]

    hist_PersonasIn.append(  response["numberOfIncoming"] )
    hist_PersonasOut.append( response["numberOfOutgoing"] )
    hist_Personas.append(    PersonasIn - PersonasOut     )



####################################

def get_CO2_msg(pred_CO2_20mins):

    start_msg = "PREDICCIÓN DE CO2 EN NIVEL "
    advice_1  = " (IDA 1). NINGUNA ACCIÓN REQUERIDA."
    advice_2  = " (IDA 2). SE RECOMIENDA VENTILAR LA OFICINA EN LOS PRÓXIMOS 15 MINUTOS"
    advice_3  = " (IDA 3). SE DEBE VENTILAR LA OFICINA EN ESTE MOMENTO"

    if   pred_CO2_20mins < 500:                             return start_msg + "OPTIMO"        + advice_1
    elif pred_CO2_20mins >= 500 and pred_CO2_20mins < 900:  return start_msg + "BUENO"         + advice_1
    elif pred_CO2_20mins >= 900 and pred_CO2_20mins < 1200: return start_msg + "ACEPTABLE"     + advice_2
    elif pred_CO2_20mins >= 1200:                           return start_msg + "DESACONSEJADO" + advice_3

def get_PM10_msg(pred_PM10_20mins):

    start_msg = "PREDICCIÓN DE PARTÍCULAS EN SUSPENSIÓN INFERIORES A 10 MICRAS EN NIVEL "
    advice_1  = ". NINGUNA ACCIÓN REQUERIDA."
    advice_2  = ". CESEN CUALQUIER POSIBLE ACTIVIDAD GENERADORA DE POLVO EN LOS PRÓXIMOS 15 MINUTOS. REVISEN EL SISTEMA DE CLIMATIZACIÓN Y VENTILACIÓN EN LAS PRÓXIMAS 48 HORAS"
    advice_3  = ". CESEN CUALQUIER POSIBLE ACTIVIDAD GENERADORA DE POLVO EN ESTE MOMENTO. REVISEN EL SISTEMA DE CLIMATIZACIÓN Y VENTILACIÓN EN LAS PRÓXIMAS 24 HORAS"

    if   pred_PM10_20mins < 20:                            return start_msg + "OPTIMO"        + advice_1
    elif pred_PM10_20mins >= 20 and pred_PM10_20mins < 40: return start_msg + "BUENO"         + advice_1
    elif pred_PM10_20mins >= 40 and pred_PM10_20mins < 60: return start_msg + "ACEPTABLE"     + advice_2
    elif pred_PM10_20mins >= 60:                           return start_msg + "DESACONSEJADO" + advice_3


def get_PM25_msg(pred_PM25_20mins):

    start_msg = "PREDICCIÓN DE PARTÍCULAS EN SUSPENSIÓN INFERIORES A 2,5 MICRAS EN NIVEL "
    advice_1  = ". NINGUNA ACCIÓN REQUERIDA."
    advice_2  = ". CESEN CUALQUIER POSIBLE ACTIVIDAD GENERADORA DE POLVO EN LOS PRÓXIMOS 15 MINUTOS. REVISEN EL SISTEMA DE CLIMATIZACIÓN Y VENTILACIÓN EN LAS PRÓXIMAS 48 HORAS"
    advice_3  = ". CESEN CUALQUIER POSIBLE ACTIVIDAD GENERADORA DE POLVO EN ESTE MOMENTO. REVISEN EL SISTEMA DE CLIMATIZACIÓN Y VENTILACIÓN EN LAS PRÓXIMAS 24 HORAS"

    if   pred_PM25_20mins < 20:                            return start_msg + "OPTIMO"        + advice_1
    elif pred_PM25_20mins >= 20 and pred_PM25_20mins < 40: return start_msg + "BUENO"         + advice_1
    elif pred_PM25_20mins >= 40 and pred_PM25_20mins < 60: return start_msg + "ACEPTABLE"     + advice_2
    elif pred_PM25_20mins >= 60:                           return start_msg + "DESACONSEJADO" + advice_3




def get_predictions(observed_array, n_points_to_predict):

    value_observed_ultimo = observed_array[-1]
    value_observed_penult = observed_array[-2]

    incremento = value_observed_ultimo - value_observed_penult # Pos->crece; Neg->decrece

    predictions = []

    current = value_observed_ultimo
    for i in range(n_points_to_predict):

        new = current + incremento # + ruido
        predictions.append(new)
        current = new

    return predictions



hist_Hora = []
hist_CO2  = []
hist_PM10 = []
hist_PM25 = []
hist_Temperatura = []
hist_Humedad = []
hist_PersonasIn = []
hist_PersonasOut = []
hist_Personas = []

show_Hora = ["Iniciando..."]
show_CO2  = [-1]
show_PM10 = [-1]
show_PM25 = [-1]
CO2_msg   = ""
PM10_msg  = ""
PM25_msg  = ""


def get_ml_predictions():

    if len(hist_Hora) >= 4:

        #### ENOUGH DATA -> DO ML PREDICTION

        show_Hora = hist_Hora[-4:] + ["+5 mins", "+10 mins", "+15 mins", "+20 mins"]
        show_PM25 = hist_PM25[-4:] + get_predictions(hist_PM25[-4:], 4)
        show_PM10 = hist_PM10[-4:] + get_predictions(hist_PM10[-4:], 4)
        show_CO2  = hist_CO2[-4:]  + get_predictions(hist_CO2[-4:],  4)
        CO2_msg   = get_CO2_msg(show_CO2[-1])
        PM10_msg  = get_PM10_msg(show_PM10[-1])
        PM25_msg  = get_PM25_msg(show_PM25[-1])

        print("ML prediction done")


    else:
        #### NO ENOUGH DATA -> ERROR MSG
        print("NO ENOUGH DATA FOR DOING ML PREDICTIONS")
        show_Hora = hist_Hora
        show_PM25 = hist_PM25
        show_PM10 = hist_PM10
        show_CO2  = hist_CO2
        PM25_msg  = "No ha transcurrido el suficienciente tiempo (<15 mins) para predecir las partículas inferiores a 2,5 micra."
        PM10_msg  = "No ha transcurrido el suficienciente tiempo (<15 mins) para predecir las partículas inferiores a 10 micras."
        CO2_msg   = "No ha transcurrido el suficienciente tiempo (<15 mins) para predecir el CO2."


####################################

def fill_data_from_HOPU_and_do_ML():

    get_datetime() # Aqui se guardan historicos horas

    ####### fill data from HOPU and save it into tmp/data.csv as a new row
    API_get_token()
    API_get_device_status(access_token)
    API_get_calidad_aire(access_token)    # Aqui se guardan historicos calidad aire
    API_get_presencia(access_token)       # Aqui se guardan historicos personas

    ####### Get last 4 rows from tmp/data.csv and do ML predictions
    get_ml_predictions()



@app.route('/')
def web_endpoint():
    data={
        "x_labels":   show_Hora,
        "CO2":        show_CO2, #[120, 153, 213, 230, 240, 220, 180, 120],
        "CO2_msg":    CO2_msg,
        "PM10":       show_PM10, #[8, 10, 20, 26, 27, 22, 13, 11],
        "PM10_msg":   PM10_msg,
        "PM25":       show_PM25, #[6, 8, 18, 23, 24, 18, 10, 8],
        "PM25_msg":   PM25_msg
    }
    return render_template('frontend.html', **data)


@app.route('/data')
def downloadData ():

    MAX = 1000
    data = {
        "hora": hist_Hora[:MAX],
        "CO2": hist_CO2 [:MAX],
        "PM10": hist_PM10[:MAX],
        "PM25": hist_PM25[:MAX],
        "Temp": hist_Temperatura[:MAX],
        "Hume": hist_Humedad[:MAX],
        "Personas": hist_Personas[:MAX],
        "PerIn": hist_PersonasIn[:MAX],
        "PerOut": hist_PersonasOut[:MAX]
    }
    return data


if __name__ == '__main__':
    
    fill_data_from_HOPU_and_do_ML()

    scheduler = BackgroundScheduler(timezone='Europe/Madrid') # Default timezone is "utc"
    #scheduler.add_job(fill_data_from_HOPU_and_do_ML, 'interval', seconds=5)
    #scheduler.add_job(fill_data_from_HOPU_and_do_ML, 'cron', day_of_week='*', hour='*', minute='*')
    scheduler.add_job(fill_data_from_HOPU_and_do_ML, 'cron', day_of_week='mon-fri', hour='7-20', minute='*/5')
    scheduler.start()

    app.run(host="0.0.0.0")

