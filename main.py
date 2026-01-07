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

class OnibusResponse(BaseModel):
    ordem: str
    linha: str
    latitude: float
    longitude: float
    velocidade: float
    status: str

@app.get("/")
async def root():
    return {"status": "online"}

@app.get("/onibus", response_model=List[OnibusResponse])
async def get_realtime_buses(linhas: str = Query(...)):
    # Normaliza as linhas para evitar problemas de comparação (ex: "0416" vs "416")
    linhas_alvo = set(l.strip().lstrip('0') for l in linhas.split(","))
    resultado = []
    
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            agora_ms = int(time.time() * 1000)
            # Aumentamos a janela para 2 minutos (120.000ms) para garantir que pegamos dados
            # A API do Rio às vezes demora a atualizar o sinal de alguns ônibus
            params = { "dataInicial": agora_ms - 120000, "dataFinal": agora_ms }
            
            print(f"DEBUG: Consultando linhas {linhas_alvo} na janela de 2min")
            response = await client.get(URL_FONTE_GPS, params=params)
            
            if response.status_code == 200:
                dados = response.json()
                veiculos = dados if isinstance(dados, list) else dados.get('veiculos', [])
                
                print(f"DEBUG: Recebidos {len(veiculos)} veículos da prefeitura")
                
                # Dicionário para manter apenas a posição mais recente de cada ônibus (pela ordem)
                mais_recentes = {}

                for bus in veiculos:
                    try:
                        # Limpeza da linha: remove ".0" e zeros à esquerda para bater com o banco
                        linha_raw = str(bus.get('linha', '')).split('.')[0].lstrip('0')
                        
                        if linha_raw in linhas_alvo:
                            ordem = bus.get('ordem')
                            # Se já vimos esse ônibus nesta resposta, mantemos apenas o mais recente
                            # (A API pode retornar múltiplas posições se a janela for grande)
                            mais_recentes[ordem] = {
                                "ordem": ordem,
                                "linha": linha_raw,
                                "latitude": float(bus['latitude'].replace(',', '.')),
                                "longitude": float(bus['longitude'].replace(',', '.')),
                                "velocidade": float(bus.get('velocidade', 0)),
                                "status": "Em movimento" if float(bus.get('velocidade', 0)) > 1 else "Parado"
                            }
                    except: continue
                
                resultado = list(mais_recentes.values())
                print(f"DEBUG: Filtrados {len(resultado)} ônibus para as linhas solicitadas")
                
    except Exception as e:
        print(f"ERRO: {e}")
        
    return resultado

@app.get("/linhas")
async def get_todas_linhas():
    if not os.path.exists(DB_NAME): return []
    conn = sqlite3.connect(DB_NAME)
    try:
        cursor = conn.execute("SELECT DISTINCT route_short_name, route_long_name FROM routes")
        return [{"numero": r[0], "nome": r[1]} for r in cursor.fetchall()]
    except: return []
    finally: conn.close()

@app.get("/linhas/{linha_numero}/shape")
async def get_shape_linha(linha_numero: str):
    if not os.path.exists(DB_NAME): return []
    conn = sqlite3.connect(DB_NAME)
    try:
        # Normaliza a linha para a busca no banco
        linha_busca = linha_numero.lstrip('0')
        cursor = conn.execute("""
            SELECT s.shape_id, s.shape_pt_lat, s.shape_pt_lon 
            FROM route_shapes s
            JOIN routes r ON s.route_id = r.route_id
            WHERE r.route_short_name = ? OR r.route_short_name = ?
        """, (linha_busca, linha_numero))
        
        shapes = {}
        for row in cursor:
            sid = row[0]
            if sid not in shapes: shapes[sid] = []
            shapes[sid].append({"latitude": row[1], "longitude": row[2]})
        return list(shapes.values())
    except: return []
    finally: conn.close()
