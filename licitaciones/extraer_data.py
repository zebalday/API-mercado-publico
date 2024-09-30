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

# Guardar licitaciones del día
API.save_licitaciones_daliy(
    dataframe = API.list_licitaciones(),
    path = PATH
)