# --- IMPORTS NECESARIOS ---
from flask import Flask, render_template, request, redirect, url_for, flash
import pandas as pd
import geopandas as gpd
import folium
from folium.plugins import Fullscreen
from branca.element import Element
import os
import logging
from datetime import datetime   
import numpy as np 

# Configuración de logging 
logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

# --- LLAVE SECRETA ---
app.secret_key = 'tu_llave_secreta_aleatoria_aqui'
# -----------------------------------------------------


# --- 1. CONFIGURACIÓN Y CARGA DE DATOS ---

GEOMETRY_PATH = 'llanquihue_comunas.geojson'
DATA_PATH = 'BASE FINAL CON CLUSTER.csv'
HPM_LOCATION = (-41.44763238267444, -72.95692300523349)

df_original = pd.DataFrame()
gdf_comunas = None

COLORES_SEVERIDAD = {'Menor': 'green', 'Moderada': 'orange', 'Mayor': 'red'}

NOMBRES_AMPUTACIONES = {
    'AMP_DEDO_MANO': 'Amputación Dedo de la Mano',
    'AMP_PULGAR': 'Amputación de Pulgar',
    'AMP_DEDO_PIE': 'Amputación Dedo del Pie',
    'AMP_A_NIVEL_PIE': 'Amputación a Nivel del Pie',
    'DESART_TOBILLO': 'Desarticulación de Tobillo',
    'AMP_NIVEL_MALEOLO': 'Amputación Nivel Maléolo',
    'AMP_DEBAJO_RODILLA': 'Amputación Debajo de la Rodilla',
    'DESART_RODILLA': 'Desarticulación de Rodilla',
    'AMP_ENCIMA_RODILLA': 'Amputación Encima de la Rodilla'
}
COLUMNAS_AMPUTACION = list(NOMBRES_AMPUTACIONES.keys())
#cambiar tanto en NOMBRES_GRUPOS como en CAMBIO DE NOMBRES CLUSTER
NOMBRES_GRUPOS = {
    'Joven': 'Joven',
    'Mayor edad': 'Mayor edad',
    'Recurrencia moderada': 'Recurrencia moderada',
    'Alta recurrencia': 'Alta recurrencia',
    'Paciente no etiquetado': 'Paciente no etiquetado' 
}


# --- Funciones de Carga ---
def load_data(csv_file_path):
    global app
    try:
        df = pd.read_csv(csv_file_path, sep=';')
    except FileNotFoundError:
        app.logger.error(f"Error: No se encontró el archivo '{csv_file_path}'.")
        return pd.DataFrame()
    
    df = df.rename(columns={
        'Sexo(Desc)': 'Sexo (Desc)',
        'UltimaEdadRegistrada': 'Ultima Edad Registrada',
        'Ultima registro severidad': 'Severidad', 
        'UltimaRegistroSeveridad': 'Severidad',    
        'ÚltimaRegistroSeveridad': 'Severidad',
        'tiempo(minutos)': 'tiempo (minutos)'
    })
    
    if 'Severidad' in df.columns:
        df['Severidad'] = df['Severidad'].astype(str).str.strip()

    # --- CAMBIO DE NOMBRES DE CLUSTER ---
    if 'Grupo' in df.columns:
        df['Grupo'] = df['Grupo'].astype(str).str.strip()
        df['Grupo'] = df['Grupo'].replace({
            'ruido': 'Paciente no etiquetado',
            'cluster 0': 'Joven',
            'cluster 1': 'Mayor edad',
            'cluster 2': 'Recurrencia moderada',
            'cluster 3': 'Alta recurrencia',
            '-1': 'Paciente No Etiquetado'
        })

    df['Comuna'] = df['Comuna'].replace({
        'Cochamo': 'Cochamó', 'Maullin': 'Maullín',
        'COCHAMO': 'Cochamó', 'MAULLIN': 'Maullín'
    })
    
    comunas_llanquihue = ['Puerto Montt', 'Calbuco', 'Cochamó', 'Fresia', 'Frutillar', 
                          'Llanquihue', 'Los Muermos', 'Maullín', 'Puerto Varas']
    
    df = df[df['Comuna'].isin(comunas_llanquihue)].copy()
    
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
    df.dropna(subset=['lat', 'lng'], inplace=True)
    
    df['Ultima Edad Registrada'] = pd.to_numeric(df['Ultima Edad Registrada'], errors='coerce')
    df.dropna(subset=['Ultima Edad Registrada'], inplace=True)
    
    return df

def load_shapes(geojson_path):
    global app
    try:
        gdf_llanquihue = gpd.read_file(geojson_path)
        gdf_llanquihue = gdf_llanquihue.to_crs(epsg=4326)
        
        nombre_columna_comuna = None
        for col in ['Comuna', 'COMUNA', 'NAME']:
            if col in gdf_llanquihue.columns:
                nombre_columna_comuna = col
                break

        if nombre_columna_comuna:
            gdf_llanquihue['Comuna_Corregida'] = gdf_llanquihue[nombre_columna_comuna]
        else:
            gdf_llanquihue['Comuna_Corregida'] = 'Comuna Desconocida'
        
        return gdf_llanquihue
    except Exception as e:
        app.logger.error(f"Error al cargar el GeoJSON: {e}")
        return None

# --- EJECUCIÓN DE CARGA ---
df_original = load_data(DATA_PATH)
gdf_comunas = load_shapes(GEOMETRY_PATH)

# --- Lógica de Filtros ---
def aplicar_filtros_comunes(args):
    global df_original
    df_filtrado = df_original.copy()
    
    if args.getlist('comuna'):
        df_filtrado = df_filtrado[df_filtrado['Comuna'].isin(args.getlist('comuna'))]

    if args.getlist('sexo'):
        df_filtrado = df_filtrado[df_filtrado['Sexo (Desc)'].isin(args.getlist('sexo'))]

    if args.getlist('grupo'):
        df_filtrado = df_filtrado[df_filtrado['Grupo'].isin(args.getlist('grupo'))]

    if args.getlist('severidad'):
        df_filtrado = df_filtrado[df_filtrado['Severidad'].isin(args.getlist('severidad'))]

    total_amp_str = args.get('total_amputaciones')
    if total_amp_str and total_amp_str.isdigit():
        df_filtrado = df_filtrado[df_filtrado['Total_Amputaciones'] == int(total_amp_str)]
        
    codigo_str = args.get('codigo_ppd') 
    if codigo_str:
        df_filtrado = df_filtrado[
            df_filtrado['Codigo'].astype(str).str.contains(codigo_str.strip(), case=False, na=False)
        ]
        
    edad_valor_str = args.get('edad_valor')
    edad_operador = args.get('edad_operador') 
    columna_edad = 'Ultima Edad Registrada'
    
    if edad_valor_str and edad_operador:
        try:
            edad_valor = float(edad_valor_str) 
            if edad_operador == '<':
                df_filtrado = df_filtrado[df_filtrado[columna_edad] < edad_valor]
            elif edad_operador == '<=':
                df_filtrado = df_filtrado[df_filtrado[columna_edad] <= edad_valor]
            elif edad_operador == '>':
                df_filtrado = df_filtrado[df_filtrado[columna_edad] > edad_valor]
            elif edad_operador == '>=':
                df_filtrado = df_filtrado[df_filtrado[columna_edad] >= edad_valor]
            elif edad_operador == '=':
                df_filtrado = df_filtrado[np.isclose(df_filtrado[columna_edad], edad_valor)] 
        except ValueError:
            pass

    amputaciones_seleccionadas = args.getlist('amputacion')
    amputacion_modo = args.get('amputacion_modo', 'OR')

    if amputaciones_seleccionadas:
        columnas_validas = [col for col in amputaciones_seleccionadas if col in COLUMNAS_AMPUTACION]
        if columnas_validas:
            if amputacion_modo == 'OR':
                condition = df_filtrado[columnas_validas].gt(0).any(axis=1) 
                df_filtrado = df_filtrado[condition]
            elif amputacion_modo == 'AND':
                condition = df_filtrado[columnas_validas].gt(0).all(axis=1)
                df_filtrado = df_filtrado[condition]
    
    return df_filtrado


# --- 3. GENERACIÓN DE MAPA (CORREGIDA: SIN HTML OCULTO) ---

def add_severity_legend(mapa):
    legend_html = """
    <div style="position: fixed; 
                 top: 20px; right: 20px; width: 150px; height: 120px; 
                 border:2px solid grey; z-index:9999; font-size:14px;
                 background-color:white; opacity:0.9; padding: 10px;">
        <b>Última Severidad</b><br>
    """
    for severity, color in COLORES_SEVERIDAD.items():
        legend_html += f"""
          <i style="background:{color}; border-radius:50%; display:inline-block; width:10px; height:10px; margin-right:5px;"></i> {severity}<br>
        """
    legend_html += "</div>"
    mapa.get_root().html.add_child(folium.Element(legend_html))

def generate_map_html(df_filtrado, HPM_LOCATION):
    global df_original, gdf_comunas
    
    cantidad_pacientes = len(df_filtrado)
    
    if df_filtrado.empty:
        mapa = folium.Map(location=HPM_LOCATION, zoom_start=9)
        folium.Marker(location=HPM_LOCATION, popup="Hospital Puerto Montt", icon=folium.Icon(color='blue', icon='hospital', prefix='fa')).add_to(mapa)
        add_severity_legend(mapa)
    else:
        lat_media = df_filtrado['lat'].mean()
        lng_media = df_filtrado['lng'].mean()
        mapa = folium.Map(location=[lat_media, lng_media], zoom_start=9, tiles="OpenStreetMap")
        Fullscreen().add_to(mapa)
        
        if gdf_comunas is not None:
            style_comunas = {'fillColor': '#222222', 'color': '#FFFFFF', 'weight': 1.5, 'fillOpacity': 0.1}
            highlight_style = {'weight': 4, 'color': 'orange', 'fillOpacity': 0.3}
            folium.GeoJson(
                gdf_comunas, name='Bordes Comunales',
                style_function=lambda x: style_comunas,
                highlight_function=lambda x: highlight_style, 
                tooltip=folium.GeoJsonTooltip(fields=['Comuna_Corregida'], aliases=['Comuna:']),
            ).add_to(mapa)
        
        folium.Marker(location=HPM_LOCATION, popup="Hospital Puerto Montt", tooltip="HPM", icon=folium.Icon(color='blue', icon='hospital', prefix='fa')).add_to(mapa)

        for _, row in df_filtrado.iterrows():
            severidad = str(row.get('Severidad', 'No Data')).strip()
            color = COLORES_SEVERIDAD.get(severidad, 'gray')
            
            # Limpieza básica del código
            raw_codigo = row['Codigo']
            try:
                if isinstance(raw_codigo, float) and raw_codigo.is_integer():
                    codigo_limpio = str(int(raw_codigo)).strip()
                else:
                    codigo_limpio = str(raw_codigo).strip()
            except:
                codigo_limpio = str(raw_codigo).strip()

            # --- POPUP HTML ESTÁNDAR (Sin cosas raras ocultas) ---
            # Esto asegura que el popup SIEMPRE se muestre bien.
            popup_html = f"""
            <div style="font-family: sans-serif; font-size: 13px; width: 200px;">
                <b>Código PPD:</b> {codigo_limpio}<br>
                <b>Comuna:</b> {row['Comuna']}<br>
                <b>Sexo:</b> {row['Sexo (Desc)']}<br>
                <b>Edad:</b> {row['Ultima Edad Registrada']}<br>
                <b>Severidad:</b> {severidad}<br>
                <hr style="margin: 5px 0;">
                <b>Tiempo HPM:</b> {row['tiempo (minutos)']:.1f} min<br>
                <b>Amputaciones:</b> {row['Total_Amputaciones']}<br>
                <b>Grupo:</b> {row['Grupo']}
            </div>
            """
            
            folium.Marker(
                location=[row['lat'], row['lng']],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"PPD: {codigo_limpio}",
                icon=folium.Icon(color=color, icon='user', prefix='fa')
            ).add_to(mapa)   

        add_severity_legend(mapa)

    html_contador = f"""
    <div style="position: fixed; top: 20px;right: 180px; width: 150px; background-color: white; border: 2px solid #333; z-index: 9999; padding: 10px; font-size: 16px; border-radius: 8px; box-shadow: 3px 3px 5px rgba(0,0,0,0.3); font-family: sans-serif;">
        <div style="text-align: center;">
            <b>Pacientes:</b><br>
            <span style="font-size: 24px; color: #007bff;">{cantidad_pacientes}</span>
        </div>
    </div>
    """
    mapa.get_root().html.add_child(folium.Element(html_contador))

    # --- SCRIPT INYECTADO: LEE EL TEXTO VISIBLE ---
    # En lugar de buscar un span oculto, leemos el texto "Código PPD: XXXXX"
    script_click = """
    <script>
        var checkMapInterval = setInterval(function() {
            var mapInstance = null;
            for (var key in window) {
                if (key.startsWith('map_') && window[key].on) {
                    mapInstance = window[key];
                    break;
                }
            }

            if (mapInstance) {
                clearInterval(checkMapInterval);
                console.log("Mapa encontrado. Listeners activados.");

                mapInstance.on('popupopen', function(e) {
                    // Obtenemos el nodo del contenido
                    var contentNode = e.popup._contentNode;
                    
                    if (contentNode) {
                        // Obtenemos todo el texto visible dentro del popup
                        var text = contentNode.innerText || contentNode.textContent;
                        
                        // Usamos Regex para extraer el código que está después de "Código PPD:"
                        var match = text.match(/Código PPD:\\s*([a-zA-Z0-9\\.]+)/);
                        
                        if (match && match[1]) {
                            var codigoPPD = match[1].trim();
                            console.log("Código extraído:", codigoPPD);
                            
                            window.parent.postMessage({
                                'type': 'marker_click',
                                'codigo': codigoPPD
                            }, "*");
                        }
                    }
                });
            }
        }, 500); 
    </script>
    """
    mapa.get_root().html.add_child(folium.Element(script_click))

    return mapa._repr_html_()


# --- 4. RUTAS ---

@app.route('/')
@app.route('/inicio')
def mostrar_inicio_y_diagrama():
    return render_template('diagrama_arquitectura.html')

@app.route('/dashboard') 
def dashboard():
    global df_original
    if df_original.empty:
        return "<h1>Error: Datos no cargados.</h1>", 500

    opciones_comuna = sorted(df_original['Comuna'].dropna().unique().tolist())
    opciones_sexo = sorted(df_original['Sexo (Desc)'].dropna().unique().tolist())
    
    # Usamos el diccionario manual para los Grupos
    opciones_grupo = NOMBRES_GRUPOS 
    
    opciones_severidad = ['Menor', 'Moderada', 'Mayor']
    
    # Usamos el diccionario manual para las Amputaciones
    opciones_amputacion = NOMBRES_AMPUTACIONES 
    
    opciones_edad_operadores = ['<', '<=', '>', '>=', '=']
    
    return render_template(
        'index.html', 
        opciones_comuna=opciones_comuna, 
        opciones_sexo=opciones_sexo,
        opciones_grupo=opciones_grupo,          
        opciones_severidad=opciones_severidad, 
        opciones_amputacion=opciones_amputacion,
        opciones_edad_operadores=opciones_edad_operadores
    )

@app.route('/get_filtered_map')
def get_filtered_map():
    df_filtrado = aplicar_filtros_comunes(request.args)
    map_html = generate_map_html(df_filtrado, HPM_LOCATION)
    return map_html

@app.route('/get_filtered_table')
def get_filtered_table():
    df_filtrado = aplicar_filtros_comunes(request.args)
    table_html = df_filtrado.to_html(
        classes="table table-striped table-hover table-bordered table-sm",
        index=False, 
        border=0,
        float_format=lambda x: '{:.2f}'.format(x) 
    )
    return table_html

@app.route('/enviar_sugerencia', methods=['POST'])
def enviar_sugerencia():
    sugerencia = request.form.get('sugerencia_texto')
    next_route = request.form.get('next_route')
    target_route = next_route if next_route in ['mostrar_inicio_y_diagrama', 'dashboard'] else 'dashboard'
    
    if sugerencia and sugerencia.strip(): 
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open('sugerencias.txt', 'a', encoding='utf-8') as f:
                f.write(f"--- {timestamp} ---\n{sugerencia}\n{'='*50}\n\n")
            flash('¡Sugerencia enviada correctamente!', 'success')
        except Exception as e:
            app.logger.error(f"Error: {e}")
            flash('Error al guardar la sugerencia.', 'error')
    else:
        flash('La sugerencia no puede estar vacía.', 'warning')
    
    return redirect(url_for(target_route)) 

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)