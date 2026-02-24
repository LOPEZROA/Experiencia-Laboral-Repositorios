# Experiencia Laboral — Repositorios (Portafolio)

Repositorio **tipo portafolio** para acompañar mi CV: aquí centralizo proyectos y desarrollos propios (código, notebooks, apps) que demuestran mi trabajo en **Ciencia de Datos**, **automatización** y **desarrollo de aplicaciones**.

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-Web%20Apps-black?style=for-the-badge&logo=flask&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-DB-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Data](https://img.shields.io/badge/Data%20Science-Analytics-orange?style=for-the-badge)

---

## 📌 Proyectos / Experiencias

| Carpeta | Qué contiene | Tecnologías principales | Acceso rápido |
|---|---|---|---|
| **[Proyecto Tesis](./Proyecto%20Tesis/)** | Proyecto integrador (PUCV + HPM): EDA, clustering y **web app geoespacial** para análisis de casos de *pie diabético* en la Provincia de Llanquihue. | Python, pandas, geopandas, folium, scikit-learn, Flask, Docker | Demo (si está disponible): `http://158.251.6.4:8699/` |
| **[Aplicación Web HCVB](./Aplicaci%C3%B3n%20Web%20HCVB/)** | Aplicación web (Flask) enfocada a procesamiento de archivos, **cálculo de medianas** y **cruces/estadísticas**. Incluye dos versiones: **Python+Excel** y **Python+PostgreSQL**. | Flask, pandas, openpyxl, PostgreSQL (psycopg2), Redis (opcional) | Ver README interno |

> Nota: este repositorio reúne proyectos con **contexto sanitario**. El código está orientado a reproducibilidad, pero los datos sensibles no deberían versionarse públicamente.

---

## 🧭 Cómo navegar este repositorio

- Cada carpeta principal tiene su propio `README.md` con:
  - Objetivo del proyecto
  - Estructura
  - Requisitos
  - Cómo ejecutar (local / despliegue)
- Si vienes desde el CV, recomiendo empezar por:
  1) **Proyecto Tesis** (end-to-end: EDA → ML → app)  
  2) **Aplicación Web HCVB** (ingeniería de datos + app productiva)

---

## 🧩 Stack (resumen)

- **Python:** pandas, numpy, openpyxl, geopandas, folium, scikit-learn  
- **Web:** Flask (templates + static)  
- **Datos:** Excel / PostgreSQL (migración y modelado)  
- **Buenas prácticas:** estructura por módulos, requisitos, y orientación a despliegue

---

## 🔒 Consideraciones de datos

- Si un proyecto utiliza información clínica/administrativa:  
  - evita subir RUT/identificadores, direcciones y cualquier dato sensible,
  - usa *data masking* y datasets sintéticos para demostraciones cuando corresponda.

---

## Autor

**Nicolás Esteban López Roa** — GitHub: [@LOPEZROA](https://github.com/LOPEZROA)
