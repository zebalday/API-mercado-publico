
""" 
********************** DESCRIPCIÓN ************************
Este script de python recopila todos los registros de la API
de api.mercadopublico.cl, los filtra en base a su contenido,
los convierte en un dataframe y los guarda en en un archivo CSV.

Tiene por objetivo almacenar registros de licitaciones
de proyectos de gestión de datos para alimentar procesos de ETL
en Talend Open Studio.

Talend luego se encarga de transformar estos datos, guardarlos
en una base de datos remota y notificar mediante correo
de los nuevos registros guardados.
***********************************************************
"""

# IMPORTAR MÓDULOS REQUERIDOS
import dotenv, os, requests, datetime, time
import pandas as pd
from pandas import DataFrame


# CARGAR VARIABLES DEL ENTORNO
dotenv.load_dotenv()
TEST_TICKET = os.getenv('TEST_TICKET')
API_TICKET = os.getenv('API_TICKET')
DATA_DIR = os.getenv('DATA_DIR')
DATAFRAME_PATH = os.getenv('DATAFRAME_PATH')


# API CLASS
class MercadoAPI():

    # ATRIBUTOS

    # URL base de API
    BASE_URL: str = 'https://api.mercadopublico.cl/servicios/v1/publico/'
    # Token para API
    TICKET: str = None



    # MÉTODOS DE CLASE

    # Constructor
    def __init__(self, ticket: str = None):

        # Inicializa atributo ticket
        self.TICKET = ticket if ticket != None else TEST_TICKET


    # Listar todos los organismo públicos
    def list_organismos(self):

        # PETICION A API
        # URL y parámetros
        URL = self.BASE_URL + 'empresas/buscarcomprador'
        response = requests.get(
            url = URL,
            params = {'ticket' : self.TICKET}
        )

        # CREAR DATAFRAME
        response = response.json()
        empresas = response['listaEmpresas']
        df = pd.DataFrame(empresas)

        print(df.head())


    # Listar licitaciones publicadas por fecha y por estado (publicada)
    # (Sin fecha --> fecha actual)
    # (Sin estado --> todos los estados)
    def list_licitaciones(self, fecha:str = ''):
        
        # PETICION A API
        # URL y parámetros
        URL = self.BASE_URL + "licitaciones"
        fecha = datetime.datetime.now().strftime('%d%m%Y') if len(fecha) == 0 else fecha
        params = {
                'ticket' : self.TICKET,
                'fecha' : fecha,
                'estado' : 'publicada'
        }
        # Petición
        response = requests.get(
            url = URL,
            params = params
        )
        # Reintentos
        intents = 10        
        while response.status_code != 200 and intents != 0:
            response = requests.get(url = URL, params = params)
            intents -= 1
            time.sleep(0.2)

        # CREACIÓN DE TABLA DE DATOS
        # Crear dataframe filtrada
        licitaciones = response.json()['Listado'] if response.status_code == 200 else []
        df = pd.DataFrame(licitaciones)
        df = df.drop(['CodigoEstado'], axis = 1) if not df.empty else df
        df = df[df['Nombre'].str.contains(r'(?i)datos')] if not df.empty else df
        # Reemplazar comas del nombre
        df['Nombre']= df['Nombre'].str.replace(',', ' ', regex = False)
        
        # AGREGAR INFORMACION ADICIONAL AL DATAFRAME
        if not df.empty:
            # Iniciar listas
            df_organismo = []
            df_region = []
            df_comuna = []
            df_fecha_creacion = []
            df_fecha_inicio = []
            df_fecha_cierre = []
            df_fecha_adjudicacion = []
            df_monto = []
            df_tipo_licitacion = []
            
            # Agregar información adicional
            for row in df.itertuples(index = True):
                codigo = str(row.CodigoExterno).strip()
                # Obtener datos
                info_licitacion = self.info_licitacion(codigo = codigo)
                # Almacenar datos
                df_organismo.append(info_licitacion['organismo'])
                df_region.append(info_licitacion['region'])
                df_comuna.append(info_licitacion['comuna'])
                df_fecha_creacion.append(info_licitacion['fecha_creacion'])
                df_fecha_inicio.append(info_licitacion['fecha_inicio'])
                df_fecha_cierre.append(info_licitacion['fecha_cierre'])
                df_fecha_adjudicacion.append(info_licitacion['fecha_adjudicacion'])
                df_monto.append(info_licitacion['monto'])
                df_tipo_licitacion.append(info_licitacion['tipo_licitacion'])
                  
            # Agregar datos adicionales a dataframe
            df['Organismo']         = df_organismo
            df['Region']            = df_region
            df['Comuna']            = df_comuna
            df['FechaCreacion']     = df_fecha_creacion
            df['FechaInicio']       = df_fecha_inicio
            df['FechaCierre']       = df_fecha_cierre
            df['FechaAdjudicacion'] = df_fecha_adjudicacion
            df['MontoEstimado']     = df_monto
            df['TipoLicitacion']     = df_tipo_licitacion

            # Reordenar dataframe
            df = df[['CodigoExterno', 
                    'TipoLicitacion',
                    'Nombre', 
                    'Organismo',
                    'Region',
                    'Comuna',
                    'FechaCreacion',
                    'FechaInicio',
                    'FechaCierre',
                    'FechaAdjudicacion',
                    'MontoEstimado'
                ]]

        # Retornar dataframe
        return (df)
        


    # Obtener informacion de licitacion por código a través de 
    # https://api.mercadopublico.cl/servicios/v1/publico/licitaciones?codigo=[CODIGO]&ticket=[TICKET]
    def info_licitacion(self, codigo: str):
        
        # PETICION A API
        # URL y parámetros
        URL = self.BASE_URL + 'licitaciones'
        params = {
                'codigo' : codigo,
                'ticket' : self.TICKET
        }
        # Petición
        response = requests.get(
            url = URL,
            params = params
        )
        # Reintentos
        intents = 10        
        while response.status_code != 200 and intents != 0:
            response = requests.get(url = URL, params = {'codigo' : codigo,'ticket' : self.TICKET})
            intents -= 1
            time.sleep(0.2)

        # ASIGNAR VALORES DESDE RESPUESTA JSON
        code                = response.status_code
        organismo           = response.json()['Listado'][0]['Comprador']['NombreOrganismo'] if code == 200 else None
        region              = response.json()['Listado'][0]['Comprador']['RegionUnidad'] if code == 200 else None
        comuna              = response.json()['Listado'][0]['Comprador']['ComunaUnidad'] if code == 200 else None
        fecha_creacion      = response.json()['Listado'][0]['Fechas']['FechaCreacion'] if code == 200 else None
        fecha_inicio        = response.json()['Listado'][0]['Fechas']['FechaInicio'] if code == 200 else None
        fecha_cierre        = response.json()['Listado'][0]['Fechas']['FechaCierre'] if code == 200 else None
        fecha_adjudicacion  = response.json()['Listado'][0]['Fechas']['FechaAdjudicacion'] if code == 200 else None
        monto               = response.json()['Listado'][0]['MontoEstimado'] if code == 200 else None
        tipo_licitacion     = response.json()['Listado'][0]['Tipo'] if code == 200 else None

        # REEMPLAZAR POSIBLES COMAS
        organismo   = organismo.replace(',', ' ')
        region      = region.replace(',', ' ')
        comuna      = comuna.replace(',', ' ')

        # RETORNAR VALORES
        return ({
            'organismo' : organismo, 
            'region' : region, 
            'comuna' : comuna, 
            'fecha_creacion' : fecha_creacion, 
            'fecha_inicio' : fecha_inicio, 
            'fecha_cierre' : fecha_cierre, 
            'fecha_adjudicacion' : fecha_adjudicacion, 
            'monto' : monto,
            'tipo_licitacion' : tipo_licitacion
        })



    # Guardar nuevas licitaciones en archivo .csv
    # utilizando la ruta DATAFRAME_PATH del entorno
    def save_licitaciones(self, dataframe: DataFrame):
        # Obtiene ruta del archivo en variable local
        path = DATAFRAME_PATH
        
        # Unir data antigua con nuevo dataframe
        try:
            data = pd.read_csv(DATAFRAME_PATH)
            init_df = pd.DataFrame(data)
            upd_df = pd.concat([init_df, dataframe], ignore_index = True)
            # Elimina registros duplicados si los hay
            upd_df.drop_duplicates(inplace = True)
            upd_df.to_csv(path, index = False)
        except:
            # Guardar nuevo dataframe
            dataframe.to_csv(path, index = False)




    