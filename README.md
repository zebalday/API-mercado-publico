# API MERCADO PÚBLICO

Este script de python recopila todos los registros de la API
de [api.mercadopublico.cl](https://api.mercadopublico.cl/), los filtra en base a su contenido,
los convierte en un dataframe y los guarda en en un archivo CSV.

Tiene por objetivo almacenar registros de licitaciones
de proyectos de gestión de datos para alimentar procesos de ETL
en Talend Open Studio.

Talend luego se encarga de transformar estos datos, guardarlos
en una base de datos remota y notificar mediante correo
de los nuevos registros guardados.




## Librerías necesarias


1. numpy==2.1.1
2. pandas==2.2.2
3. python-dotenv==1.0.1
4. requests==2.32.3
5. urllib3==2.2.2
