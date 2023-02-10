import * as dotenv from 'dotenv';
dotenv.config();
import Papa from 'papaparse';
import fs from 'fs';
import { format } from 'date-fns';
import { createLogger } from '@simalicrum/logger';
import { msgLog } from './log.js';
import { storageAccountList, storageAccountListKeys, createBlobServiceClient } from '@simalicrum/azure-helpers';

import { subscriptionId } from './config/azure.js';
import { progressFilename, logFileDir } from './config/local.js';

//Create log file
const logFileName = `${logFileDir}${format(new Date(), "yyyy-MM-dd'T'hh-mm-ss")}.log`;
const logger = createLogger(logFileName);

//Create or open progress file and create progress Map()
//This allows for resuming a partial crawl
logger.info(msgLog(`Checking for progress file`));
let progress;
if (fs.existsSync(progressFilename)) {
  logger.info(msgLog("Found progress file. Starting file scan from last scanned file markers."));
  const json = readFileSync(progressFilename, { encoding: 'utf8' });
  progress = new Map(Object.entries(JSON.parse(json)));
} else {
  logger.info(msgLog('Progress file not found. Starting file scan from the beginning'));
  progress = new Map();
}
logger.info(msgLog('Getting Azure Storage account list'));