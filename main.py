import time
import sqlite3
import httpx
from fastapi import FastAPI, Query
from typing import List, Optional
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
    conn = sqlite3.connect(DB_NAME)
    # Otimização extrema para SQLite
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA cache_size = 500") # Apenas 0.5MB de cache
    return conn

@app.get("/")
async def root():
    return {"status": "online", "mode": "on-demand"}

@app.get("/linhas")
async def get_todas_linhas():
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT route_short_name, route_long_name FROM routes")
        return [{"numero": r[0], "nome": r[1]} for r in cursor.fetchall()]
    except: return []
    finally: conn.close()

@app.get("/onibus", response_model=List[OnibusResponse])
async def get_realtime_buses(linhas: str = Query(...)):
    """
    Busca os dados na prefeitura apenas quando o app pede.
    Isso evita manter um cache gigante na memória.
    """
    linhas_alvo = set(linhas.split(","))
    resultado = []
    
    try:
        async with httpx.AsyncClient() as client:
            agora_ms = int(time.time() * 1000)
            # Janela de 40s para garantir que pegamos dados
            params = { "dataInicial": agora_ms - 40000, "dataFinal": agora_ms }
            
            # Fazemos a requisição e processamos IMEDIATAMENTE
            response = await client.get(URL_FONTE_GPS, params=params, timeout=10.0)
            
            if response.status_code == 200:
                dados = response.json()
                veiculos = dados if isinstance(dados, list) else dados.get('veiculos', [])
                
                for bus in veiculos:
                    linha = str(bus.get('linha', '')).replace('.0', '')
                    if linha in linhas_alvo:
                        resultado.append({
                            "ordem": bus.get('ordem', 'S/N'),
                            "linha": linha,
                            "latitude": float(bus['latitude'].replace(',', '.')),
                            "longitude": float(bus['longitude'].replace(',', '.')),
                            "velocidade": float(bus.get('velocidade', 0)),
                            "status": "Em movimento" if float(bus.get('velocidade', 0)) > 1 else "Parado"
                        })
                
                # Limpeza explícita para ajudar o Garbage Collector
                del veiculos
                del dados
    except Exception as e:
        print(f"Erro na consulta: {e}")
        
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
