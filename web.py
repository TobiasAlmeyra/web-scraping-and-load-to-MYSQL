# importar librerias 
import pandas as pd
from sqlalchemy import create_engine
import requests
from bs4 import BeautifulSoup
import time

# configuracion de la base de datos destino
destination_engine = create_engine('mysql+mysqlconnector://root:20041963@127.0.0.1/scraping') # BASE DE DATOS DESTINO

# FUNCION PARA CARGAR DATOS A MySQL
def load_to_mysql(df, table_name, engine):
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)  # recibe un data frame
        print(f"Datos cargados con éxito en la tabla {table_name}.")
    except Exception as e:
        print(f"Error al cargar datos a la tabla {table_name}: {e}")

# URL base del sitio web
base_url = 'https://www.fravega.com/l/tv-y-video/tv/'

# Lista para almacenar todos los nombres de los productos
todos_los_nombres = []

# Número de página inicial
pagina = 1

while True:
    # Construir la URL con el número de página
    url = f"{base_url}?page={pagina}"
    
    # Hacer la solicitud GET a la página actual
    result = requests.get(url)

    # Verificar si la solicitud fue exitosa
    if result.status_code == 200:
        content = result.text

        # Analizar el contenido HTML usando BeautifulSoup
        soup = BeautifulSoup(content, 'lxml')

        # Buscar todos los elementos que contienen la información de los productos
        elementos_a = soup.find_all('div', class_='sc-95e993ee-5 ioGeFC')

        # Verificar si se encontraron productos en la página
        if not elementos_a:
            print(f"No se encontraron más productos en la página {pagina}. Terminando.")
            break

        # Iterar sobre los elementos encontrados
        for elemento_2 in elementos_a:
            elemento_a = elemento_2.find('span', class_='sc-ca346929-0 czeMAx')
            if elemento_a:
                # Agregar el texto del elemento a la lista de nombres
                nombre_producto = elemento_a.text.strip()
                # Evitar duplicados
                if nombre_producto not in todos_los_nombres:
                    todos_los_nombres.append(nombre_producto)
        
        # Incrementar el número de página para la siguiente iteración
        pagina += 1

        # Agregar un pequeño delay para no sobrecargar el servidor
        time.sleep(1)

    else:
        print(f"Error al hacer la solicitud en la página {pagina}: {result.status_code}")
        break

# Convertir la lista de productos scrapeados en un DataFrame
df_scrapeados = pd.DataFrame(todos_los_nombres, columns=["nombre_producto"])

# Cargar los datos scrapeados a MySQL
load_to_mysql(df_scrapeados, 'contenido', destination_engine)
