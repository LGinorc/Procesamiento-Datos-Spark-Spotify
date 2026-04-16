**Proyecto de Big Data: Análisis de Spotify con Spark y Kafka**

¡Hola! Este es mi repositorio para la Tarea 3 del curso de Big Data de la UNAD. Como trabajo con temas relacionados a Spotify, quise aprovechar para hacer este ejercicio con datos musicales y hacerlo mucho más real e interesante. El proyecto está dividido en dos partes principales que son procesamiento de datos estáticos (Batch) y datos en tiempo real (Streaming).

**¿Qué hay en este repositorio?**

Básicamente, subí 3 archivos de Python que hacen toda la magia:

- `analisis_spotify.py` (La parte Batch): Aquí descargo un archivo CSV público con miles de canciones de Spotify. El código limpia los datos (quita los que están vacíos) y usa DataFrames y RDDs de PySpark para descubrir cosas como el Top 5 de géneros más populares y cuántas canciones hay por cada subgénero.
- `productor_spotify.py` (El simulador): Este es un Productor de Kafka. Básicamente simula ser la app de Spotify enviando un aviso cada segundo diciendo: "El usuario X está escuchando la canción Y por tantos segundos".
- `streaming_spotify.py` (El receptor en tiempo real): Este es el Consumidor de Spark Streaming. Se queda escuchando al productor y, en vivo, va calculando promedios de tiempo de escucha por cada género musical, agrupándolos por minuto.



**¿Cómo correr estos códigos?**

Si tienes tu máquina virtual con Ubuntu, Spark y Kafka listos, así es como se ejecutan:

1. Para ver el Análisis Batch:
Primero bajas el archivo de datos y luego corres el script:

wget https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2020/2020-01-21/spotify_songs.csv -O spotify.csv
/opt/spark/bin/spark-submit analisis_spotify.py

2. Para ver el Streaming en vivo:
Asegúrate de tener Zookeeper y Kafka prendidos. Crea el topic llamado spotify_streams:

/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic spotify_streams


3. Luego, abre dos terminales. En la primera, prende el simulador:

python3 productor_spotify.py


4. Y en la segunda, prende a Spark para que empiece a recibir y agrupar los datos:
   
/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 streaming_spotify.py


Pégalo, guarda los cambios. 
