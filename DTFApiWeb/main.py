from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
import requests
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from db import get_db
import crud

app = FastAPI()

# Middleware CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/dtf")
def get_dtf(db: Session = Depends(get_db)):
    
    # Url de la api a consumir
    url = "https://suameca.banrep.gov.co/estadisticas-economicas-back/rest/estadisticaEconomicaRestService/consultaMenuXId"
    # Parametros necesitados
    params = {"idMenu": 220003}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        return {"error": str(e)}

    # Buscar serie con id=65 (DTF)
    series = data.get("SERIES", [])
    serie_id_65 = next((s for s in series if s.get("id") == 65), None)

    if not serie_id_65:
        return {"error": "Serie DTF no encontrada"}

    # Último dato de la DTF
    valor = serie_id_65["data"][-1][1]
    time = serie_id_65["data"][-1][0]
    fecha_base = datetime.fromtimestamp(time / 1000)

    # Fechas calculadas
    fecha_desde = (fecha_base + timedelta(days=3)).strftime("%Y-%m-%d")
    fecha_hasta = (fecha_base + timedelta(days=9)).strftime("%Y-%m-%d")

    # PKs en formato int YYYYMMDD
    pk_fecha_desde = int((fecha_base + timedelta(days=3)).strftime("%Y%m%d"))
    pk_fecha_hasta = int((fecha_base + timedelta(days=9)).strftime("%Y%m%d"))

    # Guardar en BD
    #result = crud.insert_dtf(
    #    db,
    #    fecha_desde=fecha_desde,
    #    pk_fecha_desde=pk_fecha_desde,
    #    fecha_hasta=fecha_hasta,
    #    pk_fecha_hasta=pk_fecha_hasta,
    #    tasa_efectiva_anual=valor
    #)

    # Simular respuesta exitosa sin guardar
    result = {"status": "preview", "message": "⚠️ Modo lectura (no se guarda en BD)"}

    return {
        "fecha_desde": fecha_desde,
        "pk_fecha_desde": pk_fecha_desde,
        "fecha_hasta": fecha_hasta,
        "pk_fecha_hasta": pk_fecha_hasta,
        "tasa_efectiva_anual": valor,
        "status": result["status"],
        "message": result["message"]
    }