import { createClient } from '@supabase/supabase-js';  
import dotenv from 'dotenv';  

dotenv.config();  

const supabaseUrl = process.env.SUPABASE_URL;  
const supabaseKey = process.env.SUPABASE_ANON_KEY;  

if (!supabaseUrl || !supabaseKey) {  
  throw new Error('Missing Supabase credentials');  
}  

export const supabase = createClient(supabaseUrl, supabaseKey, {  
  auth: {  
    persistSession: false  
  },  
  db: {  
    schema: 'public'  
  }  
});  

// Función helper para queries con retry  
export const executeQuery = async (operation, maxRetries = 3) => {  
  let lastError;  

  for (let i = 0; i < maxRetries; i++) {  
    try {  
      const result = await operation();  
      return result;  
    } catch (error) {  
      lastError = error;  
      // Esperar antes de reintentar (exponential backoff)  
      await new Promise(resolve => setTimeout(resolve, Math.pow(2, i) * 1000));  
    }  
  }  

  throw lastError;  
};  

// Helpers para operaciones comunes  
export const dbHelpers = {  
  // Verificar si un XML ya fue procesado  
  async isXmlProcessed(emisor, folio) {  
    const { data, error } = await executeQuery(() =>   
      supabase  
        .from('processed_xml')  
        .select('count')  
        .eq('emisor', emisor)  
        .eq('folio', folio)  
        .single()  
    );  

    if (error) throw error;  
    return data?.count > 0;  
  },  

  // Insertar datos de XML procesado  
  async storeProcessedXml(xmlData) {  
    const { data, error } = await executeQuery(() =>  
      supabase  
        .from('processed_xml')  
        .upsert([xmlData], {  
          onConflict: 'emisor,folio',  
          ignoreDuplicates: true  
        })  
    );  

    if (error) throw error;  
    return data;  
  },  

  // Insertar datos XML en lote  
  async storeXmlDataBatch(batchData) {  
    const { data, error } = await executeQuery(() =>  
      supabase  
        .from('xml_data')  
        .upsert(batchData, {  
          onConflict: 'rut_emisor,folio,vlr_codigo',  
          ignoreDuplicates: false  
        })  
    );  

    if (error) throw error;  
    return data;  
  },  

  // Obtener información de documento  
  async getDocumentInfo(rutEmisor, folio) {  
    const { data, error } = await executeQuery(() =>  
      supabase  
        .from('super9_ine')  
        .select('user_email, local')  
        .eq('emisor', rutEmisor)  
        .eq('folio', folio)  
        .single()  
    );  

    if (error && error.code !== 'PGRST116') { // No data found  
      throw error;  
    }  

    return data || { user_email: null, local: null };  
  }  
};  