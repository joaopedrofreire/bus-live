from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
import json

from redis_client import redis_client
from gps_service import get_gps_sppo
from normalizacao import normalizar_registro

TTL_ONIBUS_ATIVO = 120  # segundos


def salvar_onibus_ativos(dados):
    for d in dados:
        registro = normalizar_registro(d)
        chave = f"onibus:{registro['ordem']}"

        redis_client.setex(
            chave,
            TTL_ONIBUS_ATIVO,
            json.dumps(registro)
        )


def coletar_gps():
    agora = datetime.utcnow()
    data_final = agora.strftime("%Y-%m-%d %H:%M:%S")
    data_inicial = (agora - timedelta(seconds=10)).strftime("%Y-%m-%d %H:%M:%S")

    try:
        dados = get_gps_sppo(data_inicial, data_final)
        if dados:
            salvar_onibus_ativos(dados)

        print(f"[{agora}] ônibus recebidos: {len(dados)}")

    except Exception as e:
        print(f"Erro na coleta GPS: {e}")


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(coletar_gps, "interval", seconds=10)
    scheduler.start()
