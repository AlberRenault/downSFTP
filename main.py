import paramiko
import os
import logging
import time
import json

# Configuración del logging
logging.basicConfig(
    filename='descarga_sftp.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def cargar_configuracion(ruta_config):
    """ Carga la configuración desde un archivo JSON """
    with open(ruta_config, 'r') as archivo:
        return json.load(archivo)

def verificar_archivo_local(ruta_local, sftp, archivo_remoto):
    """ Verifica si el archivo local es idéntico al remoto comparando su tamaño """
    try:
        tamaño_remoto = sftp.stat(archivo_remoto).st_size
        if os.path.exists(ruta_local) and os.path.getsize(ruta_local) == tamaño_remoto:
            logging.info(f"El archivo {archivo_remoto} ya existe en {ruta_local} y es idéntico. No se descargará.")
            return True
        return False
    except Exception as e:
        logging.error(f"Error al verificar el archivo {archivo_remoto}: {e}")
        return False

def descargar_archivo(host, puerto, usuario, clave, carpeta_remota, archivo, carpeta_destino):
    try:
        cliente = paramiko.SSHClient()
        cliente.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        cliente.connect(host, port=puerto, username=usuario, password=clave)
        sftp = cliente.open_sftp()
        sftp.chdir(carpeta_remota)
        ruta_local = os.path.join(carpeta_destino, archivo)
        ruta_remota = archivo
        if verificar_archivo_local(ruta_local, sftp, ruta_remota):
            sftp.close()
            cliente.close()
            return True
        intentos = 0
        while intentos < 5:
            try:
                sftp.get(ruta_remota, ruta_local)
                logging.info(f"Descargado {archivo} a {ruta_local}")
                if not os.path.exists(ruta_local) or os.path.getsize(ruta_local) == 0:
                    logging.warning(f"Error al descargar {archivo}. Volviendo a intentar...")
                    raise Exception("Archivo descargado vacío.")
                return True
            except Exception as e:
                intentos += 1
                logging.error(f"Ocurrió un error al descargar {archivo}: {e}")
                if intentos < 5:
                    logging.info(f"Reintentando {archivo} en 30 segundos... (Intento {intentos}/5)")
                    time.sleep(30)
                else:
                    logging.error(f"Se alcanzó el número máximo de reintentos para {archivo}. No se pudo descargar.")
        sftp.close()
        cliente.close()
        return False
    except Exception as ex:
        logging.error(f"Error en la conexión SFTP para el archivo {archivo}: {ex}")
        return False

def descargar_archivos_sftp(host, puerto, usuario, clave, carpeta_remota, prefijos, carpeta_destino):
    cliente = paramiko.SSHClient()
    cliente.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cliente.connect(host, port=puerto, username=usuario, password=clave)
    sftp = cliente.open_sftp()
    sftp.chdir(carpeta_remota)
    logging.info(f"Cambiado al directorio remoto: {carpeta_remota}")
    archivos = sftp.listdir()
    archivos_a_descargar = [archivo for archivo in archivos if any(archivo.startswith(prefijo) for prefijo in prefijos)]
    logging.info(f"Archivos a descargar: {archivos_a_descargar}")
    sftp.close()
    cliente.close()
    for archivo in archivos_a_descargar:
        logging.info(f"Iniciando descarga de: {archivo}")
        descargar_archivo(host, puerto, usuario, clave, carpeta_remota, archivo, carpeta_destino)

# Cargar la configuración
configuracion = cargar_configuracion('config.json')

# Ejecutar la función con la configuración cargada
descargar_archivos_sftp(
    configuracion['host'],
    configuracion['puerto'],
    configuracion['usuario'],
    configuracion['clave'],
    configuracion['carpeta_remota'],
    configuracion['prefijos'],
    configuracion['carpeta_destino']
)