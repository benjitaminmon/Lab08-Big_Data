# Laboratorio 08: Solución Big Data - Arquitectura Lambda

## Descripción del Proyecto
Este repositorio contiene la implementación de un ejemplo práctico de una solución Big Data en un entorno Windows, utilizando la **Arquitectura Lambda**. El proyecto simula la ingesta, el procesamiento y la consolidación de eventos (logs de plataforma) mediante dos vías simultáneas: procesamiento por lotes (Batch) y en tiempo real (Streaming).

## Tecnologías Utilizadas
* **Lenguaje principal:** Python
* **Ingesta de Datos:** Apache Kafka
* **Procesamiento (Batch y Speed):** Apache Spark (PySpark)
* **Infraestructura:** Docker Desktop
* **Capa de Servicio:** CSV / SQLite (Por definir)

## Prerrequisitos
Para ejecutar este entorno de forma local en Windows, el sistema debe contar con:
* **Python** (v3.13 o superior instalada).
* **Java JDK** (v19 o compatible instalada).
* **Docker Desktop** (en ejecución).
* **Apache Spark** configurado nativamente con sus respectivos binarios (`winutils.exe`).

## Estructura de Archivos (Fase 1 - Ingesta)
Dentro de la carpeta `Laboratorio_BigData`, se encuentran actualmente los siguientes componentes:
* `docker-compose.yml`: Archivo de configuración para levantar el contenedor con el servidor de Kafka de forma automatizada.
* `productor_kafka.py`: Script generador que simula el comportamiento de usuarios y envía eventos estructurados a Kafka a razón de 1 log por segundo.

## Instrucciones de Ejecución
Para poner en marcha la ingesta de datos, sigue estos pasos en tu terminal:

1. Levantar la infraestructura de Kafka:
   Posiciónate en la carpeta del proyecto y ejecuta:
   ```bash
   docker compose up -d
