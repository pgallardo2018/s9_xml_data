import winston from 'winston';  
import path from 'path';  
import { fileURLToPath } from 'url';  
import { dirname } from 'path';  

const __filename = fileURLToPath(import.meta.url);  
const __dirname = dirname(__filename);  

const setupLogging = () => {  
  const logDir = path.join(__dirname, '../../log');  
  const date = new Date().toISOString().slice(0, 10).replace(/-/g, '');  

  const logger = winston.createLogger({  
    level: 'info',  
    format: winston.format.combine(  
      winston.format.timestamp(),  
      winston.format.json()  
    ),  
    transports: [  
      new winston.transports.File({  
        filename: path.join(logDir, `xml_processing_${date}.log`)  
      }),  
      new winston.transports.Console({  
        format: winston.format.simple()  
      })  
    ]  
  });  

  const statsLogger = winston.createLogger({  
    level: 'info',  
    transports: [  
      new winston.transports.Console({  
        format: winston.format.simple()  
      })  
    ]  
  });  

  return { logger, statsLogger };  
};  

export const { logger, statsLogger } = setupLogging();  