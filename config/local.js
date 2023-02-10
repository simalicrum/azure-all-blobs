import * as dotenv from 'dotenv';
dotenv.config();

export const progressFilename = process.env.PROGRESS_JSON;
export const logFileDir = process.env.LOG_FILE_DIR;