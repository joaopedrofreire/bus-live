import asyncio
import time
import sqlite3
import httpx
from fastapi import FastAPI, Query
from typing import List, Dict, Optional
from pydantic import BaseModel
from contextlib import asynccontextmanager

# --- CONFIGURAÇÕES ---
URL_FONTE_GPS = "https://dados.mobilidade.rio/gps/sppo"
DB_NAME = "gtfs.db"
INTERVALO_BUSCA = 15  # Aumentado para reduzir processamento
TEMPO_EXPIRACAO = 180 # Reduzido para 3 minutos para limpar cache mais rápido

# Cache em memória otimizado (armazenamos apenas o essencial)
global_buses: Dict[str, tuple] = {} # Usando tuple para economizar memória: (linha, lat, lon, vel, last_seen)

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

class LinhaInfo(BaseModel):
    numero: str
    nome: str

# --- WORKERS ---
async def fetch_rio_data():
    """Busca dados e armazena de forma compacta."""
    async with httpx.AsyncClient() as client:
        while True:
            try:
                agora = time.time()
                agora_ms = int(agora * 1000)
                params = { "dataInicial": agora_ms - 40000, "dataFinal": agora_ms }
                
                # Usando stream ou limitando o tamanho da resposta se possível
                response = await client.get(URL_FONTE_GPS, params=params, timeout=15.0)
                
                if response.status_code == 200:
                    dados = response.json()
                    lista = dados if isinstance(dados, list) else dados.get('veiculos', [])
                    
                    # Limpa cache antigo antes de inserir novos para evitar pico de memória
                    if len(global_buses) > 2000: # Limite de segurança
                        limpar_cache_imediato(agora)

                    for bus in lista:
                        try:
                            linha = str(bus.get('linha', '')).replace('.0', '')
                            if not linha: continue
                            
                            # Armazenamos como tupla para gastar muito menos memória que um dicionário
                            global_buses[bus['ordem']] = (
                                linha,
                                float(bus['latitude'].replace(',', '.')),
                                float(bus['longitude'].replace(',', '.')),
                                float(bus.get('velocidade', 0)),
                                agora
                            )
                        except: continue
            except Exception as e:
                print(f"Erro: {e}")
            await asyncio.sleep(INTERVALO_BUSCA)

def limpar_cache_imediato(agora):
    remover = [k for k, v in global_buses.items() if agora - v[4] > TEMPO_EXPIRACAO]
    for k in remover: del global_buses[k]

async def clean_inactive_buses():
    while True:
        await asyncio.sleep(60)
        limpar_cache_imediato(time.time())

@asynccontextmanager
async def lifespan(app: FastAPI):
    task1 = asyncio.create_task(fetch_rio_data())
    task2 = asyncio.create_task(clean_inactive_buses())
    yield
    task1.cancel()
    task2.cancel()

app = FastAPI(lifespan=lifespan)

def get_db_connection():
    # Modo read-only e cache reduzido para o SQLite
    conn = sqlite3.connect(f"file:{DB_NAME}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/")
async def root():
    return {"status": "online", "active_buses": len(global_buses)}

@app.get("/linhas", response_model=List[LinhaInfo])
async def get_todas_linhas():
    conn = get_db_connection()
    try:
        # Consulta otimizada
        rows = conn.execute("SELECT route_short_name, route_long_name FROM routes").fetchall()
        return [{"numero": r[0], "nome": r[1]} for r in rows]
    except:
        return []
    finally:
        conn.close()

@app.get("/onibus", response_model=List[OnibusResponse])
async def get_realtime_buses(linhas: str = Query(...)):
    linhas_alvo = set(linhas.split(","))
    resultado = []
    agora = time.time()
    
    for ordem, data in global_buses.items():
        # data = (linha, lat, lon, vel, last_seen)
        if data[0] in linhas_alvo and (agora - data[4] < TEMPO_EXPIRACAO):
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
        # Busca direta sem carregar objetos desnecessários
        rows = conn.execute("""
            SELECT s.shape_id, s.shape_pt_lat, s.shape_pt_lon 
            FROM route_shapes s
            JOIN routes r ON s.route_id = r.route_id
            WHERE r.route_short_name = ?
            ORDER BY s.shape_id, s.shape_pt_sequence
        """, (linha_numero,)).fetchall()
        
        shapes = {}
        for row in rows:
            sid = row[0]
            if sid not in shapes: shapes[sid] = []
            shapes[sid].append({"latitude": row[1], "longitude": row[2]})
        return list(shapes.values())
    except: return []
    finally: conn.close()