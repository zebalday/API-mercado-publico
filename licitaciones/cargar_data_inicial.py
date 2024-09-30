# Importar módulos requeridos
from MercadoAPI import MercadoAPI
import dotenv
import os

# Cargar variables de entorno
dotenv.load_dotenv()
TICKET = os.getenv('API_TICKET')
PATH = os.getenv('DATAFRAME_PATH_DAILY')

# Objeto de la API
API = MercadoAPI(ticket = TICKET)

# Guardar licitaciones por fecha
API.save_licitaciones_initial(API.list_licitaciones(fecha = '26082024'), path = PATH)
API.save_licitaciones_initial(API.list_licitaciones(fecha = '27082024'), path = PATH)
API.save_licitaciones_initial(API.list_licitaciones(fecha = '28082024'), path = PATH)
API.save_licitaciones_initial(API.list_licitaciones(fecha = '29082024'), path = PATH)
API.save_licitaciones_initial(API.list_licitaciones(fecha = '30082024'), path = PATH)
API.save_licitaciones_initial(API.list_licitaciones(fecha = '02092024'), path = PATH) 
API.save_licitaciones_initial(API.list_licitaciones(fecha = '03092024'), path = PATH)
API.save_licitaciones_initial(API.list_licitaciones(fecha = '04092024'), path = PATH)
API.save_licitaciones_initial(API.list_licitaciones(fecha = '05092024'), path = PATH)
API.save_licitaciones_initial(API.list_licitaciones(fecha = '06092024'), path = PATH)
