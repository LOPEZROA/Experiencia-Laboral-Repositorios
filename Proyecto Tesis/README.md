<div align="center">

# 🏥 Proyecto Integrador: Análisis de Casos de Pie Diabético (HPM)
### Visualización de Gravedad y Amputación en la Provincia de Llanquihue

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![Data Science](https://img.shields.io/badge/Data%20Science-Analytics-orange?style=for-the-badge)
![Flask](https://img.shields.io/badge/Flask-Backend-black?style=for-the-badge&logo=flask&logoColor=white)
![Status](https://img.shields.io/badge/Estado-Finalizado-success?style=for-the-badge)

<br>

<p align="center">
  Este repositorio contiene el código fuente, notebooks de análisis y la herramienta de visualización web desarrollada para el <b>Instituto de Estadística (PUCV)</b> en colaboración con el <b>Hospital Puerto Montt (HPM)</b>.
</p>

[Ver Demo en Vivo](http://158.251.6.4:8699/) • [Documentación](#-estructura-del-repositorio) • [Metodología](#-metodología)

</div>

---

## 📂 Estructura del Repositorio

El proyecto está organizado en módulos según las fases de investigación. A continuación se detalla el contenido de cada directorio:

| Carpeta / Archivo | Descripción |
| :--- | :--- |
| **📂 `Aplicación/`** | Contiene el código fuente de la **Web App (Demo)** desarrollada en Flask. Incluye el `Dockerfile` para el despliegue y los scripts del servidor. |
| **📂 `Clustering/`** | Scripts y notebooks utilizados para la **Fase III**. Aquí se encuentran los algoritmos (K-Means, DBSCAN, OPTICS) aplicados para segmentar a los pacientes. |
| **📂 `Visualizacion/`** | Scripts de generación de mapas estáticos y gráficos exploratorios utilizados en el reporte (Fase IV). |
| **📄 `Análisis_descriptivo...`** | Notebook principal con el **Análisis Exploratorio de Datos (EDA)** de la base de datos completa (Fase II). |
| **📄 `README.md`** | Este archivo. Guía general del proyecto. |

---

## 🚀 Acceso Rápido a la Aplicación

La herramienta de visualización geoespacial permite filtrar pacientes, visualizar clusters y analizar la distribución de severidad en tiempo real.

<div align="center">

[![Ver Demo](https://img.shields.io/badge/DEMO_ONLINE-Ver_Aplicación-2ea44f?style=for-the-badge&logo=google-chrome&logoColor=white)](http://158.251.6.4:8699/)

</div>


---

## 🛠️ Tecnologías Utilizadas

El proyecto se construyó utilizando un stack de Ciencia de Datos y Desarrollo Web:

* **Lenguaje:** Python 🐍
* **Análisis y Manipulación de Datos:** `pandas`, `numpy`
* **Análisis Geoespacial:** `geopandas`, `shapely`
* **Machine Learning (Clustering):** `scikit-learn` (K-Means, DBSCAN, OPTICS)
* **Visualización:** `folium`, `matplotlib`, `seaborn`
* **Desarrollo Web:** `Flask` (Backend)
* **Despliegue:** `Docker`

---

## 📊 Metodología del Proyecto

1.  **Preprocesamiento:** Limpieza de datos, tratamiento de valores nulos y geocodificación de direcciones de pacientes en la Provincia de Llanquihue.
2.  **Análisis Exploratorio (EDA):** Estudio descriptivo de variables demográficas, severidad y tipos de amputación.
3.  **Clustering:** Aplicación del algoritmo **OPTICS** para identificar agrupaciones espaciales y perfiles de pacientes basados en densidad.
4.  **Visualización:** Desarrollo de una interfaz web para la toma de decisiones basada en mapas interactivos.

---

## 👥 Autores

**Estudiantes:**
* **Nicolás Esteban López Roa** - [@LOPEZROA](https://github.com/LOPEZROA)
* **Matías Jesús Ogaz Olguín** - [@Ogaz3021](https://github.com/Ogaz3021)

**Profesor Guía:**
* **Juan Zamora Osorio** - [@jfzo](https://github.com/jfzo)
---

<div align="center">
  <sub>Pontificia Universidad Católica de Valparaíso - Instituto de Estadística - 2025</sub>
</div>
