from datetime import datetime

def normalizar_registro(d):
    return {
        "ordem": d["ordem"],
        "linha": d.get("linha"),
        "latitude": float(d["latitude"].replace(",", ".")),
        "longitude": float(d["longitude"].replace(",", ".")),
        "velocidade": int(d.get("velocidade", 0)),
        "data_gps": int(d["datahora"]),
        "data_envio": int(d["datahoraenvio"]),
        "data_servidor": int(d["datahoraservidor"]),
        "atualizado_em": datetime.utcnow().isoformat()
    }
