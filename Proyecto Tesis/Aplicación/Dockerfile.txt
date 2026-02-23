# Usamos una base de Python más actual y robusta

FROM python:3.12-slim



# Instala dependencias del sistema requeridas por geopandas (GDAL, GEOS, etc.)

# Si la instalación de apt-get falla de nuevo, el problema es la red del servidor.

RUN apt-get update && apt-get install -y \

    build-essential \

    libgdal-dev \

    libgeos-dev \

    libproj-dev \

    python3-dev \

    gfortran \

    pkg-config \

    && rm -rf /var/lib/apt/lists/*



# Establece el directorio de trabajo

WORKDIR /app



# Copia e instala los requisitos de Python

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt



# Copia el archivo principal de la aplicación

COPY app.py .



# Puerto y comando de inicio

EXPOSE 5000

CMD ["python", "app.py"] 