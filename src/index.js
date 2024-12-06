// src/index.js  
import path, { dirname } from 'path';  
import { fileURLToPath } from 'url';  
import dotenv from 'dotenv';  
import { processXmlFiles } from './services/xmlProcessor.js';  
import { logger, statsLogger } from './utils/logger.js';  
import { supabase } from './config/supabase.js';  

const __filename = fileURLToPath(import.meta.url);  
const __dirname = dirname(__filename);  

// Cargar variables de entorno  
dotenv.config();  

// Configuración por defecto  
const DEFAULT_CONFIG = {  
  xmlDirectory: '/Users/pgallardo/development/python/Super9-InformeCarnes/xml_files',  
  batchSize: 50,  
  maxRetries: 3,  
  processCurrentMonth: true  
};  


async function initializeApp() {  
  try {  
    // Verificar conexión con Supabase  
    const { data, error } = await supabase.from('processed_xml').select('count');  
    if (error) throw new Error(`Error conectando a Supabase: ${error.message}`);  

    logger.info('Conexión a Supabase establecida correctamente');  
    return true;  
  } catch (error) {  
    logger.error(`Error de inicialización: ${error.message}`);  
    return false;  
  }  
}  

async function main() {  
  logger.info('Iniciando procesamiento de archivos XML...');  

  try {  
    // Inicializar la aplicación  
    const isInitialized = await initializeApp();  
    if (!isInitialized) {  
      logger.error('Error en la inicialización. Abortando proceso.');  
      process.exit(1);  
    }  

    // Procesar argumentos de línea de comandos  
    const args = process.argv.slice(2);  
    const config = { ...DEFAULT_CONFIG };  

    // Ejemplo: node index.js --directory=/path/to/xml --batch=100  
    args.forEach(arg => {  
      const [key, value] = arg.split('=');  
      switch (key) {  
        case '--directory':  
          config.xmlDirectory = value;  
          break;  
        case '--batch':  
          config.batchSize = parseInt(value);  
          break;  
        case '--all-months':  
          config.processCurrentMonth = false;  
          break;  
      }  
    });  

    // Iniciar procesamiento  
    const startTime = Date.now();  
    const stats = await processXmlFiles(config);  

    // Mostrar estadísticas  
    const endTime = Date.now();  
    const duration = (endTime - startTime) / 1000;  

    statsLogger.info('\n=== Estadísticas de Procesamiento ===');  
    statsLogger.info(`Duración total: ${duration.toFixed(2)} segundos`);  
    statsLogger.info(`Total de archivos encontrados: ${stats.totalFiles}`);  
    statsLogger.info(`Archivos no XML: ${stats.nonXml}`);  
    statsLogger.info(`Archivos ya procesados: ${stats.alreadyProcessed}`);  
    statsLogger.info(`Archivos de otros meses: ${stats.otherMonths}`);  
    statsLogger.info(`Errores de hash: ${stats.hashErrors}`);  
    statsLogger.info(`Sin información válida: ${stats.noValidInfo}`);  
    statsLogger.info(`Errores de procesamiento: ${stats.processingErrors}`);  
    statsLogger.info(`Procesados exitosamente: ${stats.success}`);  
    statsLogger.info('=====================================\n');  

    // Verificar si hubo errores críticos  
    if (stats.processingErrors > 0) {  
      logger.warn(`Se encontraron ${stats.processingErrors} errores durante el procesamiento`);  
    }  

  } catch (error) {  
    logger.error(`Error fatal en la aplicación: ${error.message}`);  
    process.exit(1);  
  } finally {  
    // Limpieza final  
    try {  
      // Cerrar conexiones o realizar limpieza si es necesario  
      logger.info('Finalizando aplicación...');  
    } catch (error) {  
      logger.error(`Error en la limpieza final: ${error.message}`);  
    }  
  }  
}  

// Manejo de señales de terminación  
process.on('SIGINT', async () => {  
  logger.info('Recibida señal de interrupción. Cerrando aplicación...');  
  try {  
    // Realizar limpieza necesaria  
    process.exit(0);  
  } catch (error) {  
    logger.error(`Error durante el cierre: ${error.message}`);  
    process.exit(1);  
  }  
});  

// Manejo de errores no capturados  
process.on('uncaughtException', (error) => {  
  logger.error(`Error no capturado: ${error.message}`);  
  process.exit(1);  
});  

process.on('unhandledRejection', (reason, promise) => {  
  logger.error('Promesa rechazada no manejada:', reason);  
  process.exit(1);  
});  

// Ejecutar la aplicación  
main().catch(error => {  
  logger.error(`Error en main: ${error.message}`);  
  process.exit(1);  
});  