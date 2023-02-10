import * as dotenv from 'dotenv';
dotenv.config();

export const logFileDir = process.env.LOG_FILE_DIR;
export const fileListDir = process.env.FILE_LIST_DIR;
export const destUrl = process.env.DESTINATION_URL;
export const key = process.env.AZURE_STORAGE_ACCOUNT_KEY;