import { exportDataToCsv } from '../services/csvExporter.js';
import { logger } from '../utils/logger.js';
import dotenv from 'dotenv';

// Cargar variables de entorno
dotenv.config();

async function main() {
  try {
    logger.info('Iniciando exportación de datos XML...');
    
    const xmlDirectory = '/Users/pgallardo/development/python/Super9-InformeCarnes/xml_files';
    const result = await exportDataToCsv(xmlDirectory);
    
    if (result) {
      logger.info(`Exportación completada exitosamente`);
      logger.info(`Archivo de datos: ${result.dataFile}`);
      logger.info(`Total de registros procesados: ${result.totalRecords}`);
    } else {
      logger.warn('No se encontraron datos para exportar');
    }

  } catch (error) {
    logger.error(`Error en la exportación: ${error.message}`);
    process.exit(1);
  }
}

// Ejecutar el script
main().catch(error => {
  logger.error(`Error en main: ${error.message}`);
  process.exit(1);
}); 