import os
import time
import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from prefect import flow, task, get_run_logger
from sklearn.cluster import KMeans

# Decorador para medir el tiempo de ejecución
def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        print(f"Duración de {func.__name__}: {duration:.2f} segundos")
        return result
    return wrapper

# Decorador para reportar el tamaño del directorio y mostrarlo en formato legible
def report_dir_size_decorator(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        size_bytes = get_directory_size('./data')
        size_mb = size_bytes / 1024 / 1024
        print(f"Tamaño del directorio de datos: {size_bytes} bytes ({size_mb:.2f} MB)")
        return result
    return wrapper

def get_directory_size(path='.'): 
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size

@task(retries=3, retry_delay_seconds=10)
@timing_decorator
def extract_data():
    from kaggle import KaggleApi
    try:
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files('sushilyeotiwad/wheat-seed-dataset', path='./data', unzip=True)
        print("Descarga completada.")
    except Exception as e:
        print(f"Error en la descarga: {e}")
        raise
    finally:
        print("Extracción finalizada.")

@task
@timing_decorator
def transform_data():
    try:
        df = pd.read_csv('./data/seeds_dataset.csv')
        df_clustering = df.drop('Class_(1, 2, 3)', axis=1)
        print("Transformación completada.")
        return df_clustering
    except Exception as e:
        print(f"Error en la transformación: {e}")
        raise

@task
@timing_decorator
def clustering_task(df_clustering):
    try:
        X = df_clustering[['Asymmetry_coefficient', 'Length_of_kernel_groove']]
        kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
        df_clustering['cluster'] = kmeans.fit_predict(X)
        print("Clustering completado.")
        return df_clustering, kmeans
    except Exception as e:
        print(f"Error en clustering: {e}")
        raise

@task
@timing_decorator
def plot_clusters(df_clustering, kmeans):
    try:
        X = df_clustering[['Asymmetry_coefficient', 'Length_of_kernel_groove']]
        plt.figure(figsize=(10, 8))
        scatter = plt.scatter(X['Asymmetry_coefficient'], X['Length_of_kernel_groove'], c=df_clustering['cluster'], cmap='viridis')
        # Añadir centroides
        centroids = kmeans.cluster_centers_
        plt.scatter(centroids[:, 0], centroids[:, 1], c='red', marker='X', s=200, label='Centroides')
        plt.title('K-means Clustering de Semillas (K=3)')
        plt.xlabel('Asymmetry_coefficient')
        plt.ylabel('Length_of_kernel_groove')
        plt.legend()
        plt.grid(True)
        plt.savefig('kmeans_clustering_plot_with_centroids.png')
        plt.show()
    except Exception as e:
        print(f"Error en el plot: {e}")
        raise

@task
@timing_decorator
@report_dir_size_decorator
def load_data(df_clustering):
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seeds_clusters (
                id SERIAL PRIMARY KEY,
                area FLOAT,
                perimeter FLOAT,
                compactness FLOAT,
                length_of_kernel FLOAT,
                width_of_kernel FLOAT,
                asymmetry_coefficient FLOAT,
                length_of_kernel_groove FLOAT,
                cluster INT
            );
        """)
        conn.commit()
        for _, row in df_clustering.iterrows():
            cur.execute("""
                INSERT INTO seeds_clusters (area, perimeter, compactness, length_of_kernel, width_of_kernel, asymmetry_coefficient, length_of_kernel_groove, cluster)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, tuple(row))
        conn.commit()
        cur.close()
        conn.close()
        print("Datos cargados en PostgreSQL.")
    except Exception as e:
        print(f"Error al cargar datos: {e}")
        raise

@flow
def data_pipeline():
    extract_data()
    df_clustering = transform_data()
    df_clustering, kmeans = clustering_task(df_clustering)
    plot_clusters(df_clustering, kmeans)

    load_data(df_clustering)

    # Mostrar en terminal los primeros registros de la tabla seeds_clusters
    try:
        import psycopg2
        conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host="localhost", port="5432")
        cur = conn.cursor()
        cur.execute("SELECT * FROM seeds_clusters LIMIT 5;")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        # Calcular el ancho de cada columna
        col_widths = [max(len(str(col)), max((len(str(row[i])) for row in rows), default=0)) for i, col in enumerate(colnames)]
        # Imprimir encabezado
        print("\nPrimeros registros en seeds_clusters:")
        header = " | ".join(str(col).ljust(col_widths[i]) for i, col in enumerate(colnames))
        print(header)
        print("-+-".join("-" * w for w in col_widths))
        # Imprimir filas
        for row in rows:
            print(" | ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(colnames))))
        cur.close()
        conn.close()
    except Exception as e:
        print(f"No se pudo mostrar los registros de la tabla: {e}")

# Ejecutar el pipeline
if __name__ == "__main__":
    data_pipeline()