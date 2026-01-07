import time
import sqlite3
import httpx
import os
from fastapi import FastAPI, Query
from typing import List
from pydantic import BaseModel

# --- CONFIGURAÇÕES ---
URL_FONTE_GPS = "https://dados.mobilidade.rio/gps/sppo"
DB_NAME = "gtfs.db"

app = FastAPI()

# --- MODELOS ---
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

# --- HELPER DE BANCO DE DADOS ---
def get_db_connection():
    # Verifica se o arquivo existe para evitar erros de travamento
    if not os.path.exists(DB_NAME):
        return None
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA cache_size = 500")
    return conn

@app.get("/")
async def root():
    # Endpoint de health check ultra-rápido para o Render
    return {"status": "online", "db_exists": os.path.exists(DB_NAME)}

@app.get("/linhas")
async def get_todas_linhas():
    conn = get_db_connection()
    if not conn: return []
    try:
        cursor = conn.execute("SELECT route_short_name, route_long_name FROM routes LIMIT 500")
        return [{"numero": r[0], "nome": r[1]} for r in cursor.fetchall()]
    except: return []
    finally: conn.close()

@app.get("/onibus", response_model=List[OnibusResponse])
async def get_realtime_buses(linhas: str = Query(...)):
    linhas_alvo = set(linhas.split(","))
    resultado = []
    
    try:
        # Usamos um timeout menor para não travar o worker do Render
        async with httpx.AsyncClient(timeout=5.0) as client:
            agora_ms = int(time.time() * 1000)
            params = { "dataInicial": agora_ms - 45000, "dataFinal": agora_ms }
            
            response = await client.get(URL_FONTE_GPS, params=params)
            
            if response.status_code == 200:
                dados = response.json()
                veiculos = dados if isinstance(dados, list) else dados.get('veiculos', [])
                
                for bus in veiculos:
                    linha = str(bus.get('linha', '')).replace('.0', '')
                    if linha in linhas_alvo:
                        try:
                            resultado.append({
                                "ordem": bus.get('ordem', 'S/N'),
                                "linha": linha,
                                "latitude": float(bus['latitude'].replace(',', '.')),
                                "longitude": float(bus['longitude'].replace(',', '.')),
                                "velocidade": float(bus.get('velocidade', 0)),
                                "status": "Em movimento" if float(bus.get('velocidade', 0)) > 1 else "Parado"
                            })
                        except: continue
    except Exception:
        pass
        
    return resultado

@app.get("/linhas/{linha_numero}/shape")
async def get_shape_linha(linha_numero: str):
    conn = get_db_connection()
    if not conn: return []
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
