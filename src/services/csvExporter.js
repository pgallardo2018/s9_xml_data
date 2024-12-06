import { createObjectCsvWriter } from 'csv-writer';
import path from 'path';
import { logger } from '../utils/logger.js';
import { fileURLToPath } from 'url';
import fs from 'fs/promises';
import xml2js from 'xml2js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const parseXmlFile = async (filePath) => {
  try {
    const xmlContent = await fs.readFile(filePath, 'utf-8');
    const parser = new xml2js.Parser({ explicitArray: false });
    const result = await parser.parseStringPromise(xmlContent);
    
    const documento = result.DTE.Documento;
    const encabezado = documento.Encabezado;
    const detalles = Array.isArray(documento.Detalle) ? documento.Detalle : [documento.Detalle];

    return detalles.map(detalle => ({
      fecha_emision: encabezado.IdDoc.FchEmis,
      cantidad: parseFloat(detalle.QtyItem),
      precio: parseFloat(detalle.PrcItem),
      monto: parseInt(detalle.MontoItem),
      receptor: encabezado.Receptor.RUTRecep,
      vlr_codigo: detalle.CdgItem.VlrCodigo,
      nombre_item: detalle.NmbItem,
      descripcion_item: '', 
      local: '',  // Se puede agregar si está disponible
      unidad: detalle.UnmdItem,
      tipo_dte: encabezado.IdDoc.TipoDTE,
      folio: encabezado.IdDoc.Folio,
      rut_emisor: encabezado.Emisor.RUTEmisor,
      razon_social: encabezado.Emisor.RznSoc,
      created_at: new Date().toISOString(),
      filename: path.basename(filePath)
    }));
  } catch (error) {
    logger.error(`Error procesando archivo ${filePath}: ${error.message}`);
    return [];
  }
};

const exportDataToCsv = async (xmlDirectory = '/Users/pgallardo/development/python/Super9-InformeCarnes/xml_files', outputDir = 'exports') => {
  try {
    // Crear directorio de exportación si no existe
    const exportDir = path.join(__dirname, '..', '..', outputDir);
    await fs.mkdir(exportDir, { recursive: true });

    // Leer archivos XML
    logger.info(`Leyendo archivos XML de: ${xmlDirectory}`);
    const files = await fs.readdir(xmlDirectory);
    const xmlFiles = files.filter(file => file.endsWith('.xml'));
    
    logger.info(`Encontrados ${xmlFiles.length} archivos XML`);

    // Procesar todos los archivos XML
    const allItems = [];
    for (const file of xmlFiles) {
      const filePath = path.join(xmlDirectory, file);
      const items = await parseXmlFile(filePath);
      allItems.push(...items);
      
      if (allItems.length % 1000 === 0) {
        logger.info(`Procesados ${allItems.length} items...`);
      }
    }

    if (allItems.length === 0) {
      logger.info('No se encontraron datos para exportar');
      return null;
    }

    // Generar nombre de archivo con timestamp
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const csvFilePath = path.join(exportDir, `xml_data_${timestamp}.csv`);

    // Configurar el escritor CSV
    const csvWriter = createObjectCsvWriter({
      path: csvFilePath,
      header: [
        { id: 'fecha_emision', title: 'FECHA EMISIÓN' },
        { id: 'cantidad', title: 'CANTIDAD' },
        { id: 'precio', title: 'PRECIO' },
        { id: 'monto', title: 'MONTO' },
        { id: 'receptor', title: 'RECEPTOR' },
        { id: 'vlr_codigo', title: 'CÓDIGO' },
        { id: 'nombre_item', title: 'NOMBRE ITEM' },
        { id: 'descripcion_item', title: 'DESCRIPCIÓN' },
        { id: 'local', title: 'LOCAL' },
        { id: 'unidad', title: 'UNIDAD' },
        { id: 'tipo_dte', title: 'TIPO DTE' },
        { id: 'folio', title: 'FOLIO' },
        { id: 'rut_emisor', title: 'RUT EMISOR' },
        { id: 'razon_social', title: 'RAZÓN SOCIAL' },
        { id: 'created_at', title: 'FECHA CREACIÓN' },
        { id: 'filename', title: 'ARCHIVO ORIGEN' }
      ],
      encoding: 'utf8',
      fieldDelimiter: ';'
    });

    // Escribir datos al CSV
    await csvWriter.writeRecords(allItems);
    logger.info(`CSV exportado exitosamente a: ${csvFilePath}`);

    // Generar resumen por código
    await generateSummaryByCode(allItems, exportDir, timestamp);

    return {
      dataFile: csvFilePath,
      totalRecords: allItems.length
    };

  } catch (error) {
    logger.error(`Error exportando CSV: ${error.message}`);
    throw error;
  }
};

const generateSummaryByCode = async (items, exportDir, timestamp) => {
  try {
    const summary = items.reduce((acc, item) => {
      const key = item.vlr_codigo;
      if (!acc[key]) {
        acc[key] = {
          codigo: key,
          cantidad_total: 0,
          monto_total: 0,
          precio_promedio: 0,
          conteo: 0,
          nombre_item: item.nombre_item // Guardar el nombre del primer item
        };
      }
      
      acc[key].cantidad_total += item.cantidad;
      acc[key].monto_total += item.monto;
      acc[key].conteo += 1;
      acc[key].precio_promedio = acc[key].monto_total / acc[key].cantidad_total;
      
      return acc;
    }, {});

    const csvFilePath = path.join(exportDir, `resumen_por_codigo_${timestamp}.csv`);

    const csvWriter = createObjectCsvWriter({
      path: csvFilePath,
      header: [
        { id: 'codigo', title: 'CÓDIGO' },
        { id: 'nombre_item', title: 'DESCRIPCIÓN' },
        { id: 'cantidad_total', title: 'CANTIDAD TOTAL' },
        { id: 'monto_total', title: 'MONTO TOTAL' },
        { id: 'precio_promedio', title: 'PRECIO PROMEDIO' },
        { id: 'conteo', title: 'CANTIDAD DE REGISTROS' }
      ],
      encoding: 'utf8',
      fieldDelimiter: ';'
    });

    await csvWriter.writeRecords(Object.values(summary));
    logger.info(`Resumen por código exportado a: ${csvFilePath}`);

  } catch (error) {
    logger.error(`Error generando resumen por código: ${error.message}`);
    throw error;
  }
};

export { exportDataToCsv }; 