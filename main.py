import asyncio
import time
import sqlite3
import httpx
from fastapi import FastAPI, Query, HTTPException
from typing import List, Dict, Optional
from pydantic import BaseModel
from contextlib import asynccontextmanager

# --- CONFIGURAÇÕES ---
URL_FONTE_GPS = "https://dados.mobilidade.rio/gps/sppo"
DB_NAME = "gtfs.db"
INTERVALO_BUSCA = 10  # segundos
TEMPO_EXPIRACAO = 300 # 5 minutos

# Cache em memória para os ônibus (Real-time)
global_buses: Dict[str, dict] = {}

# --- MODELOS PYDANTIC ---
class PontoCoordenada(BaseModel):
    latitude: float
    longitude: float

class OnibusResponse(BaseModel):
    ordem: str
    linha: str
    latitude: float
    longitude: float
    velocidade: float
    status: str
    direcao: Optional[float] = 0.0

class LinhaInfo(BaseModel):
    numero: str
    nome: str

# --- WORKERS ---
async def fetch_rio_data():
    async with httpx.AsyncClient() as client:
        while True:
            try:
                agora_ms = int(time.time() * 1000)
                params = { "dataInicial": agora_ms - 30000, "dataFinal": agora_ms }
                response = await client.get(URL_FONTE_GPS, params=params, timeout=10.0)
                if response.status_code == 200:
                    dados = response.json()
                    lista = dados if isinstance(dados, list) else dados.get('veiculos', [])
                    for bus in lista:
                        try:
                            ordem = bus['ordem']
                            global_buses[ordem] = {
                                "ordem": ordem,
                                "linha": str(bus.get('linha', '')).replace('.0', ''),
                                "latitude": float(bus['latitude'].replace(',', '.')),
                                "longitude": float(bus['longitude'].replace(',', '.')),
                                "velocidade": float(bus.get('velocidade', 0)),
                                "last_seen": time.time()
                            }
                        except (ValueError, KeyError):
                            continue
                    print(f"📡 GPS Atualizado: {len(lista)} veículos.")
            except Exception as e:
                print(f"❌ Erro GPS: {e}")
            await asyncio.sleep(INTERVALO_BUSCA)

async def clean_inactive_buses():
    while True:
        await asyncio.sleep(60)
        agora = time.time()
        chaves_para_remover = [k for k, v in global_buses.items() if agora - v['last_seen'] > TEMPO_EXPIRACAO]
        for k in chaves_para_remover:
            del global_buses[k]

@asynccontextmanager
async def lifespan(app: FastAPI):
    task1 = asyncio.create_task(fetch_rio_data())
    task2 = asyncio.create_task(clean_inactive_buses())
    yield
    task1.cancel()
    task2.cancel()

app = FastAPI(lifespan=lifespan)

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/")
async def root():
    return {"status": "online", "buses_in_cache": len(global_buses)}

@app.get("/linhas", response_model=List[LinhaInfo])
async def get_todas_linhas():
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT DISTINCT route_short_name, route_long_name FROM routes ORDER BY route_short_name").fetchall()
        return [{"numero": row['route_short_name'], "nome": row['route_long_name']} for row in rows]
    except Exception:
        return [{"numero": "416", "nome": "Exemplo"}, {"numero": "409", "nome": "Exemplo"}]
    finally:
        conn.close()

@app.get("/onibus", response_model=List[OnibusResponse])
async def get_realtime_buses(linhas: str = Query(..., description="Linhas separadas por vírgula")):
    linhas_alvo = linhas.split(",")
    resultado = []
    for bus in global_buses.values():
        if bus['linha'] in linhas_alvo:
            resultado.append({
                "ordem": bus['ordem'],
                "linha": bus['linha'],
                "latitude": bus['latitude'],
                "longitude": bus['longitude'],
                "velocidade": bus['velocidade'],
                "status": "Em movimento" if bus['velocidade'] > 1 else "Parado"
            })
    return resultado

@app.get("/linhas/{linha_numero}/shape", response_model=List[List[PontoCoordenada]])
async def get_shape_linha(linha_numero: str):
    conn = get_db_connection()
    try:
        rota = conn.execute("SELECT route_id FROM routes WHERE route_short_name = ?", (linha_numero,)).fetchone()
        if not rota: return []
        route_id = rota['route_id']
        rows = conn.execute("SELECT shape_id, shape_pt_lat, shape_pt_lon FROM route_shapes WHERE route_id = ? ORDER BY shape_id, shape_pt_sequence", (route_id,)).fetchall()
        shapes_dict = {}
        for row in rows:
            sid = row['shape_id']
            if sid not in shapes_dict: shapes_dict[sid] = []
            shapes_dict[sid].append({"latitude": row['shape_pt_lat'], "longitude": row['shape_pt_lon']})
        return list(shapes_dict.values())
    except Exception: return []
    finally: conn.close()