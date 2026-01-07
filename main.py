import asyncio
import time
import sqlite3
import httpx
import json
from fastapi import FastAPI, Query
from typing import List, Dict, Optional
from pydantic import BaseModel
from contextlib import asynccontextmanager

# --- CONFIGURAÇÕES ---
URL_FONTE_GPS = "https://dados.mobilidade.rio/gps/sppo"
DB_NAME = "gtfs.db"
INTERVALO_BUSCA = 20  # Aumentado para 20s para dar fôlego ao CPU/RAM
TEMPO_EXPIRACAO = 120 # Apenas 2 minutos de cache

# Cache global usando dicionário de tuplas (muito leve)
# Estrutura: { "ordem": (linha, lat, lon, vel, timestamp) }
global_buses = {}

# --- MODELOS ---
class OnibusResponse(BaseModel):
    ordem: str
    linha: str
    latitude: float
    longitude: float
    velocidade: float
    status: str

# --- WORKER OTIMIZADO ---
async def fetch_rio_data():
    """Busca dados e processa de forma a evitar picos de memória."""
    async with httpx.AsyncClient() as client:
        while True:
            try:
                agora = time.time()
                agora_ms = int(agora * 1000)
                # Janela menor (20s) para vir menos dados por vez
                params = { "dataInicial": agora_ms - 20000, "dataFinal": agora_ms }
                
                # Usamos timeout curto para não acumular requisições
                response = await client.get(URL_FONTE_GPS, params=params, timeout=10.0)
                
                if response.status_code == 200:
                    # Em vez de carregar tudo com .json(), processamos com cuidado
                    dados = response.json()
                    veiculos = dados if isinstance(dados, list) else dados.get('veiculos', [])
                    
                    # Limpeza preventiva do cache
                    if len(global_buses) > 1500:
                        chaves = list(global_buses.keys())
                        for k in chaves:
                            if agora - global_buses[k][4] > TEMPO_EXPIRACAO:
                                del global_buses[k]

                    for bus in veiculos:
                        try:
                            ordem = bus.get('ordem')
                            linha = str(bus.get('linha', '')).replace('.0', '')
                            if not ordem or not linha: continue
                            
                            # Armazenar apenas o estritamente necessário
                            global_buses[ordem] = (
                                linha,
                                float(bus['latitude'].replace(',', '.')),
                                float(bus['longitude'].replace(',', '.')),
                                float(bus.get('velocidade', 0)),
                                agora
                            )
                        except: continue
                    
                    # Forçar coleta de lixo do Python se necessário (opcional)
                    del veiculos
                    del dados
                    
            except Exception:
                pass # Silencioso para economizar logs/CPU
            
            await asyncio.sleep(INTERVALO_BUSCA)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(fetch_rio_data())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

def get_db_connection():
    # Otimização máxima para SQLite em ambiente de pouca RAM
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA cache_size = 1000") # Limita cache do SQLite a ~1MB
    return conn

@app.get("/")
async def root():
    return {"active": len(global_buses)}

@app.get("/linhas")
async def get_todas_linhas():
    conn = get_db_connection()
    try:
        # Busca apenas o necessário
        cursor = conn.execute("SELECT route_short_name, route_long_name FROM routes")
        return [{"numero": r[0], "nome": r[1]} for r in cursor.fetchall()]
    except: return []
    finally: conn.close()

@app.get("/onibus", response_model=List[OnibusResponse])
async def get_realtime_buses(linhas: str = Query(...)):
    linhas_alvo = set(linhas.split(","))
    resultado = []
    agora = time.time()
    
    # Itera sobre uma cópia das chaves para evitar erro de mutação
    for ordem in list(global_buses.keys()):
        data = global_buses.get(ordem)
        if data and data[0] in linhas_alvo:
            if agora - data[4] < TEMPO_EXPIRACAO:
                resultado.append({
                    "ordem": ordem,
                    "linha": data[0],
                    "latitude": data[1],
                    "longitude": data[2],
                    "velocidade": data[3],
                    "status": "Em movimento" if data[3] > 1 else "Parado"
                })
    return resultado

@app.get("/linhas/{linha_numero}/shape")
async def get_shape_linha(linha_numero: str):
    conn = get_db_connection()
    try:
        cursor = conn.execute("""
            SELECT s.shape_id, s.shape_pt_lat, s.shape_pt_lon 
            FROM route_shapes s
            JOIN routes r ON s.route_id = r.route_id
            WHERE r.route_short_name = ?
        """, (linha_numero,))
        
        shapes = {}
        for row in cursor:
            sid = row[0]
            if sid not in shapes: shapes[sid] = []
            shapes[sid].append({"latitude": row[1], "longitude": row[2]})
        return list(shapes.values())
    except: return []
    finally: conn.close()