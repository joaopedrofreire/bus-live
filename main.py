import sqlite3
import os
import random
from fastapi import FastAPI, Query
from typing import List, Dict
from pydantic import BaseModel

# --- CONFIGURAÇÕES ---
DB_NAME = "gtfs.db"

app = FastAPI()

class OnibusResponse(BaseModel):
    ordem: str
    linha: str
    latitude: float
    longitude: float
    velocidade: float
    status: str

# Estado global para a simulação: { "linha": [ { "ordem": "...", "index_no_shape": 0 } ] }
simulacao_onibus: Dict[str, List[dict]] = {}
# Cache de shapes para não ler o banco toda hora: { "linha": [ (lat, lon), ... ] }
cache_shapes: Dict[str, List[tuple]] = {}

def get_db_connection():
    if not os.path.exists(DB_NAME): return None
    return sqlite3.connect(DB_NAME)

def carregar_shape(linha: str):
    """Busca o primeiro shape disponível para a linha no banco."""
    if linha in cache_shapes: return cache_shapes[linha]
    
    conn = get_db_connection()
    if not conn: return []
    try:
        cursor = conn.execute("""
            SELECT s.shape_pt_lat, s.shape_pt_lon 
            FROM route_shapes s
            JOIN routes r ON s.route_id = r.route_id
            WHERE r.route_short_name = ?
            ORDER BY s.shape_id, s.shape_pt_sequence
            LIMIT 200
        """, (linha,))
        pontos = cursor.fetchall()
        if pontos:
            cache_shapes[linha] = pontos
            return pontos
    except: pass
    finally: conn.close()
    return []

@app.get("/")
async def root():
    return {"status": "simulacao_ativa", "linhas_simuladas": list(simulacao_onibus.keys())}

@app.get("/onibus", response_model=List[OnibusResponse])
async def get_simulated_buses(linhas: str = Query(...)):
    linhas_alvo = [l.strip() for l in linhas.split(",")]
    resultado = []
    
    for linha in linhas_alvo:
        shape = carregar_shape(linha)
        if not shape: continue
        
        # Se a linha ainda não tem ônibus simulados, cria 3 ônibus em pontos aleatórios
        if linha not in simulacao_onibus:
            simulacao_onibus[linha] = [
                {"ordem": f"SIM-{linha}-{i}", "idx": random.randint(0, len(shape)-1)}
                for i in range(3)
            ]
        
        # Atualiza a posição de cada ônibus (move para o próximo ponto do shape)
        for bus in simulacao_onibus[linha]:
            bus["idx"] = (bus["idx"] + 1) % len(shape)
            ponto = shape[bus["idx"]]
            
            resultado.append({
                "ordem": bus["ordem"],
                "linha": linha,
                "latitude": ponto[0],
                "longitude": ponto[1],
                "velocidade": 40.0,
                "status": "Simulado"
            })
            
    return resultado

@app.get("/linhas")
async def get_todas_linhas():
    conn = get_db_connection()
    if not conn: return []
    try:
        cursor = conn.execute("SELECT DISTINCT route_short_name, route_long_name FROM routes LIMIT 100")
        return [{"numero": r[0], "nome": r[1]} for r in cursor.fetchall()]
    except: return []
    finally: conn.close()

@app.get("/linhas/{linha_numero}/shape")
async def get_shape_linha(linha_numero: str):
    shape = carregar_shape(linha_numero)
    if not shape: return []
    # Retorna no formato esperado pelo app: List[List[Ponto]]
    return [[{"latitude": p[0], "longitude": p[1]} for p in shape]]
