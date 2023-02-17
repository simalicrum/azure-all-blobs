import * as dotenv from 'dotenv';
dotenv.config();
import Papa from 'papaparse';
import fs from 'fs';
import { Blob } from 'node:buffer';
import { format } from 'date-fns';
import { v4 as uuidv4 } from 'uuid';
import { createLogger } from '@simalicrum/logger';
import { msgLog } from './log.js';
import { parse, stringify } from 'yaml'
import { program } from "commander";
import { storageAccountList, storageAccountListKeys, listBlobsFlat, listContainers, createBlobFromLocalPath } from '@simalicrum/azure-helpers';

import { subscriptionId } from './config/azure.js';
import { logFileDir, fileListDir, destUrl, key } from './config/local.js';


program.option("-i, --yaml-input-file <file>", "Specify accounts and containers to scan rather than all files under a given subscription");

program.option("-u, --upload-to-azure", "Upload file list CSVs to Azure URL indicated in DESTINATION_URL environment variable");

program.parse(process.argv);

const options = program.opts();

// Create log file
const logFileName = `${logFileDir}${format(new Date(), "yyyy-MM-dd'T'hh-mm-ss")}.log`;
const logger = createLogger(logFileName);

let filters;
if (options.yamlInputFile) {
  const yaml = fs.readFileSync(options.yamlInputFile, { encoding: 'utf-8' });
  filters = parse(yaml);
}

// console.log(filters);

// Get the full list of Azure Blob Storage accounts under a given
// subscription ID then get the storage account keys
logger.info(msgLog('Getting Azure Storage account list'));
const storageAccountsAsync = storageAccountList(subscriptionId);
const storageAccounts = [];
for await (const storageAccount of storageAccountsAsync) {
  if (options.yamlInputFile) {
    if (filters.accounts.some(element => element.name === storageAccount.name)) {
      storageAccounts.push(storageAccount);
    }
  } else {
    storageAccounts.push(storageAccount);
  }
}

const storageAccountsWithKeys = [];
for (const storageAccount of storageAccounts) {
  const [match, resourceGroup] = storageAccount.id.match(/\/resourceGroups\/(.*?)\//);
  const keyList = await storageAccountListKeys(subscriptionId, resourceGroup, storageAccount.name);
  storageAccountsWithKeys.push(
    {
      name: storageAccount.name,
      id: storageAccount.id,
      keys: keyList.keys,
      resourceGroup,
      containers: []
    }
  )
  logger.info(msgLog(`Found keys for storage account ${storageAccount.name} of resource group ${resourceGroup}`));
}

for (const storageAccount of storageAccountsWithKeys) {
  logger.info(msgLog(`Starting file scan on storage account ${storageAccount.name}.`));
  for await (const container of listContainers(storageAccount.name, storageAccount.keys[0].value)) {
    if (options.yamlInputFile) {
      const accountFilter = filters.accounts.find(element => element.name === storageAccount.name);
      if (accountFilter.hasOwnProperty('containers')) {
        if (!accountFilter.containers.some(element => element === container.name)) {
          continue;
        }
      }
    }
    const base = uuidv4();
    let outputPath;
    logger.info(msgLog(`Starting file scan on container ${container.name}.`));
    let bytes = 98;
    let volume = 0;
    for await (const res of listBlobsFlat(storageAccount.name, storageAccount.keys[0].value, container.name).byPage({ maxPageSize: 5000 })) {
      const blobs = res.segment.blobItems.map(
        blob =>
        ({
          name: blob.name,
          account: storageAccount.name,
          container: container.name,
          ResourceType: blob.properties.ResourceType || null,
          createdOn: blob.properties.createdOn,
          lastModified: blob.properties.lastModified,
          contentLength: blob.properties.contentLength,
          contentMD5: blob.properties.contentMD5 ? blob.properties.contentMD5.toString('base64') : null,
          accessTier: blob.properties.accessTier
        })
      )
      // Format blob info into CSV format at write it to file
      const rows = Papa.unparse(blobs, { header: false, delimiter: ',' });
      // Azure Search Service is limited to 
      bytes += new Blob([rows]).size;
      if (bytes > 16000000) {
        volume++
        bytes = new Blob([rows]).size;
      }
      outputPath = `${fileListDir}${storageAccount.name}/${container.name}/${base}-${volume}.csv`;
      if (!fs.existsSync(outputPath)) {
        fs.mkdirSync(`${fileListDir}${storageAccount.name}/${container.name}/`, { recursive: true });
        fs.writeFileSync(outputPath, 'name,account,container,ResourceType,createdOn,lastModified,contentLength,contentMD5,accessTier\r\n');
      }
      fs.appendFileSync(outputPath, `${rows}\r\n`, { encoding: 'utf8' });
    }
    // Copy the local blob listing CSV to blob storage
    const destBlob = `${destUrl}${outputPath}`;
    console.log("destBlob: ", destBlob);
    console.log("outputPath: ", outputPath);
    const res = await createBlobFromLocalPath(destBlob, key, outputPath, {}, () => { });
    if (res === 201) {
      logger.info(msgLog(`Output CSV successfully written to blob storage: ${destBlob}`));
    } else if (res === 200) {
      logger.info(msgLog(`Skipped writting output CSV, file size is equal: ${destBlob}`));
    } else {
      console.log(res);
      throw new Error("Couldn't write blob to destination");
    }
  }
}
