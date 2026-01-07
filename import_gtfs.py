import pandas as pd
import sqlite3
import os

# Caminho onde estão seus arquivos .txt
GTFS_PATH = "./gtfs_files/" 
DB_NAME = "gtfs.db"

def carregar_gtfs():
    print("⏳ Lendo arquivos GTFS...")
    
    # 1. Carregar Dataframes
    routes = pd.read_csv(os.path.join(GTFS_PATH, "routes.txt"))
    trips = pd.read_csv(os.path.join(GTFS_PATH, "trips.txt"))
    shapes = pd.read_csv(os.path.join(GTFS_PATH, "shapes.txt"))
    stops = pd.read_csv(os.path.join(GTFS_PATH, "stops.txt"))
    stop_times = pd.read_csv(os.path.join(GTFS_PATH, "stop_times.txt"))

    # Conectar ao SQLite
    conn = sqlite3.connect(DB_NAME)
    
    print("🛠 Processando e simplificando dados...")

    # --- TABELA 1: ROTAS (ROUTES) ---
    # Salva as rotas como estão
    routes.to_sql("routes", conn, if_exists="replace", index=False)

    # --- TABELA 2: SHAPES UNIFICADOS ---
    # Lógica: Uma rota tem várias trips, e trips têm shapes. 
    # Vamos pegar todos os shapes únicos associados a uma route_id.
    
    # Faz o Join de Trips com Shapes para saber qual shape pertence a qual rota
    trips_shapes = trips[['route_id', 'shape_id', 'direction_id']].drop_duplicates()
    
    # Merge com os pontos do shape
    shapes_completo = pd.merge(trips_shapes, shapes, on='shape_id')
    
    # Ordenar para garantir o desenho correto da linha
    shapes_completo.sort_values(by=['route_id', 'shape_id', 'shape_pt_sequence'], inplace=True)
    
    shapes_completo.to_sql("route_shapes", conn, if_exists="replace", index=False)
    
    # --- TABELA 3: PARADAS POR ROTA ---
    # Descobrir quais paradas pertencem a qual rota
    # Join: routes -> trips -> stop_times -> stops
    
    # Pegamos apenas trips únicas por rota (para não duplicar processamento)
    unique_trips = trips[['route_id', 'trip_id']].drop_duplicates(subset=['route_id'])
    
    # Pegamos os stop_times dessas trips
    stops_na_rota = pd.merge(unique_trips, stop_times[['trip_id', 'stop_id', 'stop_sequence']], on='trip_id')
    
    # Pegamos os detalhes da parada (nome, lat, lon)
    stops_final = pd.merge(stops_na_rota, stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']], on='stop_id')
    
    stops_final.to_sql("route_stops", conn, if_exists="replace", index=False)

    # Criar índices para deixar a busca rápida na API
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_shapes_route ON route_shapes (route_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_stops_route ON route_stops (route_id)")
    conn.commit()
    conn.close()
    
    print(f"✅ Sucesso! Banco de dados '{DB_NAME}' criado.")

if __name__ == "__main__":
    carregar_gtfs()