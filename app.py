# Run the application (as long as is named app.py) with:
# $ flask run 
# $ python app.py 

import requests
from flask import Flask, render_template, send_file
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime
import pytz
import os
import numpy as np
import pandas as pd


app = Flask(__name__)

hora_list = ["Iniciando..."]
CO2_list  = [-1]
PM10_list = [-1]
PM25_list = [-1]
CO2_msg  = ""
PM10_msg = ""
PM25_msg = ""


#################################### READ HOPU DATA

def get_datetime():
    # datetime object containing current date and time
    # dd/mm/YY H:M:S
    global date,time,hora
    dt = datetime.now(pytz.timezone("Europe/Madrid"))
    date = dt.strftime("%d/%m/%Y")
    time = dt.strftime("%H:%M:%S")
    hora = dt.strftime("%H:%M")


# 1. Iniciar sesion en el APIRest de Hopu
#    Obtener access token y refress token

def API_get_token():
    url     = "https://fiware.hopu.eu/keycloak/auth/realms/fiware-server/protocol/openid-connect/token" 
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data    = "username=julgonzalez&password=vZnAWE7FexwgEqwT&grant_type=password&client_id=fiware-login"
    response = requests.post(url, data = data, headers = headers).json()

    global access_token, refresh_token

    access_token  = response["access_token"]
    refresh_token = response["refresh_token"]


def API_get_device_status(access_token):

    url     = "https://fiware.hopu.eu/orion/v2/entities?limit=1000&attrs=*,dateModified&options=count,keyValues" 
    headers = {"fiware-service": "Device", "fiware-servicepath": "/ctcon", "Authorization": "Bearer "+access_token}
    response = requests.get(url, headers = headers).json()[0]

    global operationalStatus

    operationalStatus = response["operationalStatus"]


def API_get_calidad_aire(access_token):

    url     = "https://fiware.hopu.eu/orion/v2/entities?limit=1000&attrs=*,dateModified&options=keyValues" 
    headers = {"fiware-service": "AirQuality", "fiware-servicepath": "/ctcon", "Authorization": "Bearer "+access_token}
    response = requests.get(url, headers = headers).json()[0]

    global CO2,PM10,PM25,Temperatura,Humedad    

    CO2         = response["CO2"]
    PM10        = response["PM10"]
    PM25        = response["PM25"]
    Temperatura = response["temperature"]
    Humedad     = response["humidity"]



def API_get_presencia(access_token):

    url     = "https://fiware.hopu.eu/orion/v2/entities?limit=1000&attrs=*,dateModified&options=count,keyValues" 
    headers = {"fiware-service": "PeopleCounting", "fiware-servicepath": "/ctcon", "Authorization": "Bearer "+access_token}
    response = requests.get(url, headers = headers).json()[0]

    global PersonasIn,PersonasOut,Personas

    PersonasIn  = response["numberOfIncoming"]
    PersonasOut = response["numberOfOutgoing"]
    Personas    = PersonasIn - PersonasOut



####################################

def init_empty_data():
    f = open("tmp/data.csv", "w")
    f.write("Fecha,Hora,PersonasIn,PersonasOut,Personas,Temperatura,Humedad,CO2,PM10,PM25\n")
    f.close()

def save_data():
    f = open('tmp/data.csv', 'a')
    f.write(date+","+hora+","+
            str(PersonasIn)+","+
            str(PersonasOut)+","+
            str(Personas)+","+
            str(Temperatura)+","+
            str(Humedad)+","+
            str(CO2)+","+
            str(PM10)+","+
            str(PM25)+"\n")
    f.close()

def print_data():
    print("   PersonasIn  = ", PersonasIn)
    print("   PersonasOut = ", PersonasOut)
    print("   Personas    = ", Personas)
    print("   Temperatura = ", Temperatura)
    print("   Humedad     = ", Humedad)
    print("   CO2         = ", CO2)
    print("   PM10        = ", PM10)
    print("   PM25        = ", PM25)



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



def get_ml_predictions():

    global hora_list, CO2_list, PM10_list, PM25_list, CO2_msg, PM10_msg, PM25_msg
    global ml_model

    # get tail(4) that means lag15, lag10, lag5, actual 
    in_dat = pd.read_csv("tmp/data.csv").tail(4)

    hora_hist = in_dat["Hora"].values
    temp_hist = in_dat["Temperatura"].values # np array [temp_lag15, temp_lag10, temp_lag5, temp_actual] 
    hume_hist = in_dat["Humedad"].values
    pm25_hist = in_dat["PM25"].values
    pm10_hist = in_dat["PM10"].values
    CO2_hist  = in_dat["CO2"].values
    pers_hist = in_dat["Personas"].values

    if len(in_dat) == 4:

        #### ENOUGH DATA -> DO ML PREDICTION

        hora_list = list(hora_hist) + ["+5 mins", "+10 mins", "+15 mins", "+20 mins"]
        PM25_list = list(pm25_hist) + get_predictions(pm25_hist, 4)
        PM10_list = list(pm10_hist) + get_predictions(pm10_hist, 4)
        CO2_list  = list(CO2_hist)  + get_predictions(CO2_hist, 4)
        CO2_msg   = get_CO2_msg(CO2_list[-1])
        PM10_msg  = get_PM10_msg(PM10_list[-1])
        PM25_msg  = get_PM25_msg(PM25_list[-1])

        print("ML prediction done")


    else:
        #### NO ENOUGH DATA -> ERROR MSG
        print("NO ENOUGH DATA FOR DOING ML PREDICTIONS")
        hora_list = list(hora_hist)
        PM25_list = list(pm25_hist)
        PM10_list = list(pm10_hist)
        CO2_list  = list(CO2_hist)
        PM25_msg  = "No ha transcurrido el suficienciente tiempo (<15 mins) para predecir las partículas inferiores a 2,5 micra."
        PM10_msg  = "No ha transcurrido el suficienciente tiempo (<15 mins) para predecir las partículas inferiores a 10 micras."
        CO2_msg   = "No ha transcurrido el suficienciente tiempo (<15 mins) para predecir el CO2."


####################################

def fill_data_from_HOPU_and_do_ML():

    global date, time

    get_datetime()
    print("pipeline at " + date + " " + time)

    ####### fill data from HOPU and save it into tmp/data.csv as a new row
    API_get_token()
    API_get_device_status(access_token)
    API_get_calidad_aire(access_token)    
    API_get_presencia(access_token)
    print_data()
    save_data()

    ####### Get last 4 rows from tmp/data.csv and do ML predictions
    get_ml_predictions()



@app.route('/')
def web_endpoint():
    global hora_list, CO2_list, PM10_list, PM25_list, CO2_msg, PM10_msg, PM25_msg
    data={
        "x_labels":   hora_list,
        "CO2":        CO2_list, #[120, 153, 213, 230, 240, 220, 180, 120],
        "CO2_msg":    CO2_msg,
        "PM10":       PM10_list, #[8, 10, 20, 26, 27, 22, 13, 11],
        "PM10_msg":   PM10_msg,
        "PM25":       PM25_list, #[6, 8, 18, 23, 24, 18, 10, 8],
        "PM25_msg":   PM25_msg
    }
    return render_template('frontend.html', **data)


@app.route('/data')
def downloadData ():
    return send_file("tmp/data.csv", as_attachment=True)


if __name__ == '__main__':

    init_empty_data()
    init_ml_model()
    
    scheduler = BackgroundScheduler(timezone='Europe/Madrid') # Default timezone is "utc"
    #scheduler.add_job(fill_data_from_HOPU_and_do_ML, 'interval', seconds=5)
    #scheduler.add_job(fill_data_from_HOPU_and_do_ML, 'cron', day_of_week='*', hour='*', minute='*')
    scheduler.add_job(fill_data_from_HOPU_and_do_ML, 'cron', day_of_week='mon-fri', hour='7-20', minute='*/5')
    scheduler.start()

    port = os.getenv('PORT') # Port is given by Heroku as environmental variable
    print("Port:", port)

    #app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)
    app.run(host="0.0.0.0")

