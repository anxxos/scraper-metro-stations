# -*- coding: utf-8 -*-
'''
Por: Ángeles Blanco y Ana Rodríguez

Este script contiene dos spiders que extraen toda la información disponible 
en el CRTM de líneas, estaciones y accesibilidades del metro y metro ligero 
de Madrid. 
Posteriormente, la información es integrada en los archivos GTFS de estos 
mismos medios de transporte y el output final es un fichero de formato .csv
con toda la información de ambas fuentes (DATOS.csv).
'''

# Importamos librerías

import scrapy
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
import pandas as pd
import numpy as np
import json as js
import os

#------------------------------------------------------------------------------
# Borramos archivos anteriores .json por si no es la primera vez que se ejecuta
# este script. De esta forma, no se concatenarán los datos y no causarán 
# errores.
try:
    os.remove('metro.json')
    os.remove('ligero.json')
except Exception:
    pass

#------------------------------------------------------------------------------
# ¡Comenzamos!
# Obtenemos información

# LÍNEAS, ESTACIONES Y ACCESIBILIDAD DE METRO
class MetroscrapySpider(scrapy.Spider):
    name = 'MetroScrapy'
    allowed_domains = ['www.crtm.es']
    start_urls = ['http://www.crtm.es/tu-transporte-publico/metro/lineas.aspx']
    
    def parse(self, response):

        # Seguimos links a páginas de líneas:
        for href in response.css("div ul li a::attr(href)")\
                    .re(r'/tu-transporte-publico/metro/lineas/.*'):
            
            yield response.follow(href, self.parse_linea)
    
    # Sacamos estaciones ordenadas de cada línea:       
    def parse_linea(self, response):   
        
        # Seguimos links a páginas de estaciones:
        for href in response.css("td a::attr(href)")\
                    .re(r'/tu-transporte-publico/metro/estaciones/.*'):
             
            yield response.follow(href, self.parse_estacion)

        # Extraemos información de estaciones: 
        
        # Definimos función útil para extracción:
        def extract_with_css(query):
            return response.css(query).extract()
        
        line = response.css("div h4.titu4::text").extract()[0]
        number_line = response.css("div h4.titu4 span::text").extract()[0]
        stops_ = number_line + "_" + line

        #cada 'key' del diccionario será el código de la línea
        self.estaciones = [y[-1].split('.')[0] \
                        for y in [x.split('/') \
                        for x in response.css("td a::attr(href)")\
                        .re(r'/tu-transporte-publico/metro/estaciones/.*')]]
        
        yield {stops_: self.estaciones}
        
    # Sacamos accesibilidad de cada estación:
    def parse_estacion(self, response):
        #station_name = response.css("div h2.titu1::text").extract()[3][6:-4]
        
        #cada 'key' del diccionario será el código de la estación
        station_code = (response.url.split('/')[-1]).split('.')[0]

        yield {station_code: response.css("div p::text")\
                                            .re(r'Estación.*')}
        
# LÍNEAS, ESTACIONES Y ACCESIBILIDAD DE METRO LIGERO
class MetroLigeroscrapySpider(scrapy.Spider):
    name = 'MetroLigeroScrapy'
    allowed_domains = ['www.crtm.es']
    start_urls = ['http://www.crtm.es/tu-transporte-publico/metro-ligero/lineas.aspx']
        
    def parse(self, response):
        
        # Seguimos links a páginas de líneas:
        for href in response.css("div ul li a::attr(href)")\
                    .re(r'/tu-transporte-publico/metro-ligero/lineas/.*'):

            yield response.follow(href, self.parse_linea)
    
    # Sacamos estaciones ordenadas de cada línea:       
    def parse_linea(self, response):
        
        #Seguimos links a páginas de estaciones:
        for href in response.css("td a::attr(href)")\
            .re(r'/tu-transporte-publico/metro-ligero/estaciones/.*'):
                
                yield response.follow(href, self.parse_estacion)
                
        #Extraemos información de estaciones:
        
        # Definimos función útil para extracción:
        def extract_with_css(query):
            return response.css(query).extract()
        
        line = response.css("div h4.titu4::text").extract()[0]
        number_line = response.css("div h4.titu4 span::text").extract()[0]
        stops_ = number_line + "_" + line

        #cada 'key' del diccionario será el código de la línea
        self.estaciones_ligero = [y[-1].split('.')[0] \
                        for y in [x.split('/') \
                        for x in response.css("td a::attr(href)")\
                        .re(r'/tu-transporte-publico/metro-ligero/estaciones/.*')]]
        
        yield {stops_: self.estaciones_ligero}
    
    # Sacamos accesibilidad de cada estación:
    def parse_estacion(self, response):
        #por si queremos el nombre de la estación
        #station_name = response.css("div h2.titu1::text").extract()[3][6:-4]
        
        #cada 'key' del diccionario será el código de la estación
        station_code = (response.url.split('/')[-1]).split('.')[0]
        
        yield {station_code: response.css("div p::text").re(r'Estación.*')}

#------------------------------------------------------------------------------
# Automatizamos proceso de guardado de datos

# Cambiamos el fichero 'settings' para que guarde los resultados en .json:
settings_metro = get_project_settings()
settings_metro.overrides['FEED_FORMAT'] = 'json'
settings_metro.overrides['FEED_URI'] = 'metro.json'

settings_ligero = get_project_settings()
settings_ligero.overrides['FEED_FORMAT'] = 'json'
settings_ligero.overrides['FEED_URI'] = 'ligero.json'

# Definimos clases para ejecutar múltiples 'crawlers' y que se inicialice
# el 'reactor':
configure_logging()
runner_metro = CrawlerRunner(settings_metro)
runner_ligero = CrawlerRunner(settings_ligero)

# Ejecutamos los 'spiders' de manera secuencial (encadenando los 'deferreds')
# y paramos el 'reactor' cuando finalice la extracción:
@defer.inlineCallbacks
def crawl():
    yield runner_metro.crawl(MetroscrapySpider)
    yield runner_ligero.crawl(MetroLigeroscrapySpider)
    reactor.stop()

crawl()
#el script se bloqueará aquí hasta que el último 'crawler' haya finalizado
reactor.run() 

#------------------------------------------------------------------------------
# Juntamos la información 'scrapeada' con los ficheros stops.txt de ambos
# medios de transporte

# Importamos archivos y parseamos .json:
metro_stops_ = pd.read_csv("stops.txt", delimiter=",")
metro_lig_stops = pd.read_csv("stops_ligero.txt", delimiter=",")

with open("metro.json", "r") as file:
    result_metro = file.read()
file.close()

with open("ligero.json", "r") as file:
    result_ligero = file.read()
file.close()

result_metro_parsed = js.loads(result_metro, encoding = 'utf-8')
result_ligero_parsed = js.loads(result_ligero, encoding = 'utf-8')

# Juntamos datos:
frames = [metro_stops_, metro_lig_stops]
metro_stops = pd.concat(frames, ignore_index = True)

result_metro_parsed.extend(result_ligero_parsed)
result_total = result_metro_parsed

# Inicializamos variables:
metro = pd.DataFrame()
estaciones = []
orden = []
nombre_lineas = []
codigo_lineas = []
access = pd.DataFrame()
estacion_acceso = []
accesibilidad = []

# Definimos función útil para buscar palabras en listas:
def search_word(x, palabra):
    a = 0
    if x == []:
        a = 0
    else:
        if palabra in x:
            a = 1
        else:
            a = 0
    return a

# Sacamos listas de estaciones, orden de estaciones según la línea, líneas y
# accesibilidades:
for linea in np.arange(0,len(result_total)):
   for key, values in result_total[linea].items():
       if '_ ' in key:
           for j in np.arange(0,len(values)):
               estaciones.append(values[j])
               orden.append(j+1)
               nombre_lineas.append(key.split('_ ')[1])
               codigo_lineas.append(key.split('_ ')[0])
       else:
           estacion_acceso.append(key)
           accesibilidad.append(values)

# Creamos nuevos dataframes con toda la información 'scrapeada':
# Estaciones, líneas y orden
metro['stop_id'] = estaciones
metro['order_id'] = orden 
metro['line_name'] = nombre_lineas
metro['line_id'] = codigo_lineas

# Estaciones, accesibilidades y tipo de transporte
access['stop_id'] = estacion_acceso
access['accessibility'] = accesibilidad
access['estacion_accesible'] = access['accessibility'].map(lambda x: 
    search_word(x, 'Estación accesible'))  
access['cobertura_movil'] = access['accessibility'].map(lambda x: 
    search_word(x, 'Estación con cobertura móvil'))  
access['escaleras_mecanicas'] = access['accessibility'].map(lambda x: 
    search_word(x, 'Estación con escaleras mecánicas'))
access['ascensores'] = access['accessibility'].map(lambda x: 
    search_word(x, 'Estación con ascensor'))    
access['wifi gratuito'] = access['accessibility'].map(lambda x: 
    search_word(x, 'Estación con Wifi gratuito'))  
access['transport_name'] = access['stop_id'].map(lambda x: 
    np.where('4_' in x, 'metro', 'metro_ligero'))

# Juntamos ambos dataframes:
merger = metro.merge(access, how='left', on='stop_id').drop('accessibility',
                    axis=1)

# Añadimos columna con código de las estaciones, para poder juntar tanto 
# paradas como estaciones y accesos:
metro_stops['stop_id2'] = metro_stops['stop_id'].map(lambda x: 
    x.split('_')[1]+'_'+x.split('_')[2])

# Juntamos información de los .txt con la información 'scrapeada':
merger_definitivo = merger.merge(metro_stops, how='left',
                                 left_on='stop_id', right_on='stop_id2')
merger_definitivo['transport_code'] = merger_definitivo['stop_id2'].map(lambda x: 
    x.split('_')[0]) # código del medio de transporte
merger_definitivo = merger_definitivo.drop(['stop_id_x','stop_id2'], axis = 1)

# Cambiamos formato del id de la línea para ordenar también las líneas:
def change_type(x):
    try: 
        x = int(x)
    except Exception:
        x = str(x)    
    return x

merger_definitivo['line_id'] = merger_definitivo['line_id'].map(lambda x: 
    change_type(x))
merger_definitivo = merger_definitivo.sort_values(['transport_name', 'line_id'])

# Cambiamos nombres de algunas columnas:
merger_definitivo.rename(columns = {'stop_id_y': 'stop_id'}, inplace = True)

# Añadimos 'estaciones' con código diferente a la página de CRTM; es decir,
# estaciones con stop_id que comience por 'est_90'. Estas estaciones engloban
# a otras estaciones y se enlazan por 'parent_stop'.
stops_90 = metro_stops_[metro_stops_.stop_id.map(lambda x: x.startswith('est_90'))]
frames_2 = [merger_definitivo, stops_90]
merger_definitivo = pd.concat(frames_2)

# Reordenamos columnas:
cols = merger_definitivo.columns.tolist()
cols = cols[4:-3] + cols[0:4] + cols[-3:]
merger_definitivo = merger_definitivo[cols]


# Pasamos los resultados a un archivo .csv:
merger_definitivo.to_csv('DATOS.csv', encoding = 'utf-8', index = False)

#------------------------------------------------------------------------------
# Et voilà!