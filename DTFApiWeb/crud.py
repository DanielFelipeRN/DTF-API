from sqlalchemy.orm import Session
from sqlalchemy import text

def insert_dtf(
    db: Session,
    fecha_desde: str,
    pk_fecha_desde: int,
    fecha_hasta: str,
    pk_fecha_hasta: int,
    tasa_efectiva_anual: float
):
    
    #Verifica si existe
    check_query = text("""
        SELECT COUNT(*) FROM FACT_DTF AS count
        WHERE PK_FECHA_DESDE = :pk_fecha_desde AND PK_FECHA_HASTA = :pk_fecha_hasta
    """)
    result = db.execute(check_query, {
        "pk_fecha_desde": pk_fecha_desde,
        "pk_fecha_hasta": pk_fecha_hasta
    }).scalar_one()

    if result > 0:
        return {"status": "exists", "message": "⚠️ El registro ya existe."}

    insert_query = text("""
        INSERT INTO FACT_DTF_SEMANAL_PRUEBA (
            FECHA_DESDE,
            PK_FECHA_DESDE,
            FECHA_HASTA,
            PK_FECHA_HASTA,
            TASA_EFECTIVA_ANUAL
        )
        VALUES (
            :fecha_desde,
            :pk_fecha_desde,
            :fecha_hasta,
            :pk_fecha_hasta,
            :tasa_efectiva_anual
        )
    """)

    db.execute(insert_query, {
        "fecha_desde": fecha_desde,
        "pk_fecha_desde": pk_fecha_desde,
        "fecha_hasta": fecha_hasta,
        "pk_fecha_hasta": pk_fecha_hasta,
        "tasa_efectiva_anual": tasa_efectiva_anual
    })
    db.commit()
    return {"status": "success", "message": "✅ Guardado con éxito."}