import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import { cpus } from 'os';
import { supabase } from '../config/supabase.js';  
import { logger } from '../utils/logger.js';  
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import xml2js from 'xml2js';
import crypto from 'crypto';
import { createWriteStream } from 'fs';

// Obtener __filename y __dirname para ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Función para obtener datos de super9_ine
const getSuper9IneData = async (folio, emisor) => {
  try {
    const { data, error } = await supabase
      .from('super9_ine')
      .select('local, user_email')
      .eq('folio', folio)
      .eq('emisor', emisor)
      .single();

    if (error) {
      logger.error(`Error obteniendo datos de super9_ine: ${error.message}`);
      return { local: '', user_email: '' };
    }

    return data || { local: '', user_email: '' };
  } catch (error) {
    logger.error(`Error en getSuper9IneData: ${error.message}`);
    return { local: '', user_email: '' };
  }
};

// Al inicio del archivo, después de los imports
const ALLOWED_CODES = new Set([
  'P16', 'P37', 'p61', 'P01', 'P59', 'P73', 'P70', 'O03', 'P31',
    '1010267', '1010130', '1012685', '1010105', '1010220', '1010130', '1012684', '1010078', '1010002', '1010096', '1012655', '1010048', '1010220', '1010034', '1010695',
    '1040074', '1040004', '1040152', '1040147', '1040591', '1040146', '1040629', '1040551', '1040552', '1040145', '1040553', '1040852', '1040631', '1040628', '1040359',
    '1040486', '1040720', '1040037', '1040017', '1040129', '1040019', '1040005', '1040128', '1040824', '1040170', '1040173', '1040135', '1040172', '1040494', '1040286',
    '1040285', '1040014', '1040304', '1040021', '20', '27', '30', '102', '151', '210', '215', '265', '282', '399', '403', '604', '606', '734', '738', '864', '904', '915',
    '916', '1006', '1018', '1019', '1020', '1032', '1039', '1053', '1064', '1066', '1111', '1228', '1249', '1350', '1374', '1388', '1397', '1402', '1404', '1409', '1416',
    '1421', '1429', '1433', '1451', '1680', '1695', '1831', '1834', '3003', '3004', '3012', '3014', '3017', '3018', '3019', '3022', '3023', '3027', '3032', '3034', '3038',
    '3044', '3048', '3054', '3058', '3074', '3097', '3108', '3109', '3153', '3154', '3201', '3239', '3241', '3242', '3243', '3256', '3288', '3300', '3309', '3313', '3316',
    '3456', '3460', '3519', '3536', '3558', '3599', '3600', '3607', '3611', '3627', '3638', '3639', '3640', '3850', '3851', '3852', '3853', '3854', '3897', '3947', '3948',
    '3951', '3960', '3961', '3971', '3976', '3979', '3984', '3986', '3987', '8080', '8082', '8088', '8117', '8206', '8207', '8210', '8213', '8237', '8299', '8300', '8451',
    '8453', '8454', '8459', '8462', '8463', '8508', '8516', '8594', '8630', '8753', '8777', '2260', '2534', '2537', '2536', '2700', 'VV3111', '302', 'VV3121', 'VV3131', 'VV1392',
    '2437', '2496', '1597', '1688', 'VV3472', '1805', 'VV3201', '280', '2747', '478', '477', 'VV4021', '1X000', '2C000', '3F000', '3I000', '4W000', '6J000', 'A4000', 'A9000',
    'AJ000', 'AL000', 'AR000', 'AY000', 'B7000', 'BI000', 'BL000', 'C9000', 'CAR01', 'CJP24', 'CJP25', 'CJR01', 'CJR02', 'CJR12', 'CJR13', 'CJR16', 'CJR28', 'CJR29', 'CJR31',
    'CJR32', 'CJR33', 'CJR34', 'CLP02', 'CLR04', 'CLR05', 'CLR07', 'CLR16', 'CLR18', 'CLR19', 'CLR20', 'CLR21', 'CLR22', 'CLR23', 'CLR29', 'CLR30', 'CLR31', 'CLR32', 'CLR33',
    'CLR35', 'CMR00', 'CS000', 'CSP00', 'CSP03', 'CSP04', 'CSR00', 'CSR10', 'CSR11', 'CSR14', 'CVR01', 'CVR04', 'CVR05', 'D9000', 'EB000', 'EER02', 'FI000', 'FL000', 'FR000',
    'FY000', 'H9000', 'HR000', 'IT000', 'J6000', 'JE000', 'JH000', 'JL000', 'JP000', 'JS000', 'JW000', 'K6000', 'K9000', 'L5000', 'LR000', 'LW000', 'LX000', 'NZ000', 'O8000',
    'OA000', 'OK000', 'P3000', 'P9000', 'PG000', 'QE001', 'QP000', 'QU000', 'R8000', 'RM000', 'RQ000', 'RT000', 'S6000', 'SQ000', 'TA000', 'TK000', 'TL000', 'V1000', 'V2000',
    'V6000', 'VC000', 'VE000', 'VF000', 'VH000', 'VJ000', 'WY000' , '3L000', 'JG000', 'AG000', 'GA000', 'P9000', 'CSP01', 'PO000', 'X6000'
]);

const processXmlFile = async (filePath, xmlContent) => {
  try {
    // Verificar si el archivo ya fue procesado
    const filename = path.basename(filePath);
    const contentHash = generateContentHash(xmlContent);
    
    const { data: processed } = await supabase
      .from('processed_xml')
      .select('filename')
      .eq('filename', filename)
      .single();

    if (processed) {
      logger.info(`Archivo ${filename} ya fue procesado anteriormente`);
      return null;
    }

    if (!xmlContent.trim()) {
      throw new Error('El archivo XML está vacío');
    }

    const parser = new xml2js.Parser({ 
      explicitArray: false,
      mergeAttrs: true,
      trim: true,
      explicitRoot: true
    });
    const result = await parser.parseStringPromise(xmlContent);

    const documento = result?.DTE?.Documento;
    if (!documento) {
      throw new Error('Estructura XML inválida: No se encontró DTE.Documento');
    }

    // Extraer datos del encabezado
    const encabezado = documento.Encabezado;
    const detalles = Array.isArray(documento.Detalle) ? documento.Detalle : [documento.Detalle];

    // Obtener datos de super9_ine usando el folio y emisor
    const folio = encabezado.IdDoc.Folio;
    const emisor = encabezado.Emisor.RUTEmisor;
    const super9IneData = await getSuper9IneData(folio, emisor);

    if (!super9IneData.local || !super9IneData.user_email) {
      logger.warn(`No se encontraron datos en super9_ine para folio: ${folio}, emisor: ${emisor}`);
    }

    // Función auxiliar para obtener el código INT1
    const getVlrCodigo = (cdgItem) => {
      if (!cdgItem) return '';
      
      // Si es un array
      if (Array.isArray(cdgItem)) {
        const item = cdgItem.find(c => c.TpoCodigo === 'INT1');
        return item ? item.VlrCodigo : '';
      }
      
      // Si es un objeto único
      return cdgItem.TpoCodigo === 'INT1' ? cdgItem.VlrCodigo : '';
    };

    // Procesar cada línea de detalle y filtrar por códigos permitidos
    const items = detalles
      .map(detalle => {
        const vlrCodigo = getVlrCodigo(detalle.CdgItem);
        // Solo procesar si el código está en la lista permitida
        if (!ALLOWED_CODES.has(vlrCodigo)) {
          return null;
        }
        
        return {
          fecha_emision: encabezado.IdDoc.FchEmis,
          cantidad: parseFloat(detalle.QtyItem),
          precio: parseFloat(detalle.PrcItem),
          monto: parseInt(detalle.MontoItem),
          receptor: encabezado.Receptor.RUTRecep,
          vlr_codigo: vlrCodigo,
          nombre_item: detalle.NmbItem,
          descripcion_item: '', 
          local: super9IneData.local,
          unidad: detalle.UnmdItem,
          tipo_dte: encabezado.IdDoc.TipoDTE,
          folio: folio,
          rut_emisor: emisor,
          razon_social: encabezado.Emisor.RznSoc,
          created_at: new Date().toISOString(),
          user_email: super9IneData.user_email
        };
      })
      .filter(item => item !== null);

    // Agregar el filename como metadata pero no como parte del item
    return items.length > 0 ? items.map(item => ({ ...item, _filename: filename })) : null;

  } catch (error) {
    logger.error(`Error procesando XML ${filePath}: ${error.message}`);
    return null;
  }
};

// Función auxiliar para generar el hash del contenido
const generateContentHash = async (content) => {
  return crypto.createHash('sha256').update(content).digest('hex');
};

// Función para verificar si el archivo ya fue procesado
const isFileProcessed = async (filename, hash) => {
  try {
    const { data, error } = await supabase
      .from('processed_xml')
      .select('id')
      .eq('filename', filename)
      .eq('hash_contenido', hash);

    if (error) {
      logger.error(`Error verificando archivo procesado: ${error.message}`);
      return false;
    }

    // Verificar si hay algún registro
    return data && data.length > 0;
  } catch (error) {
    logger.error(`Error en isFileProcessed: ${error.message}`);
    return false;
  }
};

// Función para verificar si existe el registro emisor-folio
const checkEmitterFolioExists = async (emisor, folio) => {
  const { data, error } = await supabase
    .from('processed_xml')
    .select('id')
    .eq('emisor', emisor)
    .eq('folio', folio);

  if (error) {
    logger.error(`Error verificando emisor-folio: ${error.message}`);
    return false;
  }

  return data && data.length > 0;
};

// Función para registrar el archivo procesado
const registerProcessedFile = async (fileInfo) => {
  const { error } = await supabase
    .from('processed_xml')
    .insert([fileInfo]);

  if (error) {
    throw new Error(`Error registrando archivo procesado: ${error.message}`);
  }
};

const processFilesInParallel = async (files) => {
  const numCPUs = cpus().length - 1;
  const chunkSize = Math.ceil(files.length / numCPUs);
  const chunks = [];

  for (let i = 0; i < files.length; i += chunkSize) {
    chunks.push(files.slice(i, i + chunkSize));
  }

  const workers = chunks.map((chunk) => {
    return new Promise((resolve, reject) => {
      const worker = new Worker(__filename, {
        workerData: { files: chunk.map(file => file.toString()) }
      });

      worker.on('message', resolve);
      worker.on('error', reject);
      worker.on('exit', (code) => {
        if (code !== 0) {
          reject(new Error(`Worker stopped with exit code ${code}`));
        }
      });
    });
  });

  return Promise.all(workers);
};

const processXmlFiles = async (config) => {
  if (isMainThread) {
    try {
      const dirPath = config.xmlDirectory;
      logger.info(`Procesando archivos en: ${dirPath}`);
      
      // Crear archivo de log para duplicados
      const logPath = path.join(path.dirname(dirPath), 'duplicados.log');
      const logStream = createWriteStream(logPath, { flags: 'a' });
      const logDuplicado = (mensaje) => {
        logStream.write(`${new Date().toISOString()} - ${mensaje}\n`);
      };

      const files = await fs.readdir(dirPath);
      const xmlFiles = files.filter(file => file.endsWith('.xml'));
      const fullPaths = xmlFiles.map(file => path.join(dirPath, file));
      
      logger.info(`Encontrados ${fullPaths.length} archivos XML`);
      
      // Estadísticas
      let stats = {
        totalFiles: fullPaths.length,
        nonXml: files.length - xmlFiles.length,
        alreadyProcessed: 0,
        otherMonths: 0,
        hashErrors: 0,
        noValidInfo: 0,
        processingErrors: 0,
        success: 0,
        duplicateItems: 0
      };

      // Procesar archivos
      const results = await processFilesInParallel(fullPaths);
      const allItems = results.flat();
      logger.info(`Procesados ${allItems.length} items totales (incluyendo duplicados)`);

      // Agrupar items por clave única y registrar duplicados
      const uniqueItems = allItems.reduce((acc, item) => {
        const key = `${item.rut_emisor}-${item.folio}-${item.vlr_codigo}`;
        if (!acc[key] || new Date(item.created_at) > new Date(acc[key].created_at)) {
          if (acc[key]) {
            logDuplicado(`Reemplazado: ${key} - Archivo: ${item._filename}`);
          }
          acc[key] = item;
        } else {
          stats.duplicateItems++;
          logDuplicado(`Duplicado descartado: ${key} - Archivo: ${item._filename}`);
        }
        return acc;
      }, {});

      // Convertir a array y hacer upsert (removiendo el campo _filename)
      const dedupedItems = Object.values(uniqueItems).map(({ _filename, ...item }) => item);
      logger.info(`Insertando ${dedupedItems.length} items únicos (${stats.duplicateItems} duplicados eliminados)`);
      
      // Insertar datos
      const { error: upsertError } = await supabase
        .from('xml_data')
        .upsert(dedupedItems, {
          onConflict: 'rut_emisor,folio,vlr_codigo'
        });

      if (upsertError) throw upsertError;

      // Registrar archivos procesados
      const processedFiles = fullPaths
        .filter(filePath => {
          const filename = path.basename(filePath);
          const items = allItems.some(item => item._filename === filename);
          if (!items) {
            logger.info(`Archivo ${filename} no contiene productos válidos`);
          }
          return items;
        })
        .map(filePath => ({
          filename: path.basename(filePath),
          created_at: new Date().toISOString(),
          hash_contenido: generateContentHash(fs.readFileSync(filePath, 'utf-8')),
          status: 'success'
        }));

      if (processedFiles.length > 0) {
        const { error: processedError } = await supabase
          .from('processed_xml')
          .upsert(processedFiles, {
            onConflict: 'filename',
            ignoreDuplicates: true
          });

        if (processedError) {
          logger.error(`Error registrando archivos procesados: ${processedError.message}`);
          stats.processingErrors++;
        } else {
          logger.info(`Registrados ${processedFiles.length} archivos en processed_xml`);
        }
      }

      // Cerrar archivo de log
      logStream.end();
      logger.info(`Log de duplicados guardado en: ${logPath}`);

      // Actualizar estadísticas finales
      stats.success = dedupedItems.length;
      stats.processingErrors = upsertError ? 1 : stats.processingErrors;

      return stats;

    } catch (error) {
      logger.error(`Error procesando archivos XML: ${error.message}`);
      throw error;
    }
  }
};

// Código del worker
if (!isMainThread) {
  const processWorkerFiles = async () => {
    const { files } = workerData;
    const results = [];

    for (const filePath of files) {
      try {
        const content = await fs.readFile(filePath, 'utf-8');
        const items = await processXmlFile(filePath, content);
        if (items) results.push(...items);
      } catch (error) {
        logger.error(`Error processing file ${filePath}: ${error.message}`);
      }
    }

    parentPort.postMessage(results);
  };

  processWorkerFiles();
}

export { processXmlFiles };  