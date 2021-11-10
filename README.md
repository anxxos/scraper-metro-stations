# SCRAPER del Consorcio Regional de Transportes de Madrid (CRTM)
## Obtención de datos de las estaciones de Metro y Metro Ligero

###### Diciembre 2018

Los ficheros GTFS que proporciona el CRTM en formato abierto no contienen el orden del itinerario de las líneas, ni las líneas a las que pertenece cada estación; es necesario buscar esta información realizando un scraper en su [website](http://www.crtm.es/tu-transporte-publico.aspx). Este scraper accede al enlace Líneas de cada medio de transporte. Una vez allí, recorre todas las líneas, accediendo a su contenido, con el fin de obtener la ordenación de las estaciones y asociar cada estación con la línea a la que pertenece. Además, para cada estación recoge los elementos de accesibilidad de que dispone. Por ejemplo, la estación [Pinar de Chamartín](https://www.crtm.es/tu-transporte-publico/metro/estaciones/4_263.aspx) tiene como Servicios Escaleras Mecánicas y Ascensores.

Posteriormente se integran las dos fuentes de datos (GTFS y datos 'scrapeados') en un fichero de texto en formato CSV con toda la información obtenida: el medio de transporte, la línea, el conjunto de estaciones en el orden marcado por el itinerario de cada línea y los elementos de accesibilidad de cada estación.

#### Ejecución del programa
La manera óptima de ejecutar este proyecto es desde una terminal (recomendable Anaconda Prompt). 

1. Situarse en la carpeta que contiene el 'spider' del proyecto: 

>>> cd ..\metro_spyder\metro_spyder\spiders

2. Escribir el siguiente comando: 

>>> python MetroSpider.py

3. Cuando el 'spider' termine de efectuar el 'scraping'
de las 'urls' requeridas y se ejecute el resto del código,
se creará un fichero con toda la información 'scrapeada' 
(CRTM) en conjunto con la información proveniente de la
fuente de datos GTFS, denominado DATOS.csv. Dicho fichero
se encontrará dentro de la carpeta 'spiders'.
