# Importar módulos requeridos
from MercadoAPI import MercadoAPI
import dotenv
import os

# Cargar variables de entorno
dotenv.load_dotenv()
TICKET = os.getenv('API_TICKET')

# Objeto de la API
API = MercadoAPI(ticket = TICKET)

# Guardar licitaciones por fecha
API.save_licitaciones(API.list_licitaciones(fecha = '26082024'))
API.save_licitaciones(API.list_licitaciones(fecha = '27082024'))
API.save_licitaciones(API.list_licitaciones(fecha = '28082024'))
API.save_licitaciones(API.list_licitaciones(fecha = '29082024'))
API.save_licitaciones(API.list_licitaciones(fecha = '30082024'))
API.save_licitaciones(API.list_licitaciones(fecha = '02092024'))

# Guardar licitaciones del día
#API.save_licitaciones(API.list_licitaciones())