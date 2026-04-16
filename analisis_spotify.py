from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, avg, round

# 1. Crear sesión
spark = SparkSession.builder.appName("AnalisisSpotify").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Cargar datos
df = spark.read.csv("spotify.csv", header=True, inferSchema=True)
total_original = df.count()

# 3. Limpieza de datos (Eliminar nulos en columnas clave)
df_limpio = df.dropna(subset=["track_name", "track_artist", "track_popularity", "playlist_genre"])
total_limpio = df_limpio.count()

print("=====================================================")
print(f" LIMPIEZA DE DATOS (SPOTIFY) ")
print(f"Registros originales cargados: {total_original}")
print(f"Registros útiles tras limpieza: {total_limpio}\n")

# 4. Análisis Exploratorio (DataFrames)
print(" 1. TOP 5 GÉNEROS MÁS POPULARES (PROMEDIO) ")
df_limpio.groupBy("playlist_genre").agg(round(avg("track_popularity"), 2).alias("avg_popularity")).orderBy(desc("avg_popularity")).show()

print(" 2. TOP 5 ARTISTAS CON MÁS CANCIONES EN LAS PLAYLISTS ")
df_limpio.groupBy("track_artist").count().orderBy(desc("count")).show(5)

# 5. Análisis con RDD (Transformaciones y Acciones)
print(" 3. ANÁLISIS CON RDD: CONTEO DE CANCIONES POR SUBGÉNERO ")
# Extraemos la columna subgénero, la mapeamos a pares (subgénero, 1) y reducimos por llave
rdd_subgenres = df_limpio.select("playlist_subgenre").rdd.map(lambda row: (row[0], 1))
conteo_subgenres = rdd_subgenres.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False)

for subgenre, cantidad in conteo_subgenres.take(5):
    print(f"Subgénero: {subgenre} -> Total canciones: {cantidad}")
print("=====================================================\n")

spark.stop()
