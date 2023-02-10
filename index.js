import * as dotenv from 'dotenv';
dotenv.config();
import Papa from 'papaparse';
import fs from 'fs';
import { format } from 'date-fns';
import { createLogger } from '@simalicrum/logger';
import { msgLog } from './log.js';
import { storageAccountList, storageAccountListKeys, listBlobsFlat, listContainers, createBlobFromLocalPath } from '@simalicrum/azure-helpers';

import { subscriptionId } from './config/azure.js';
import { logFileDir, fileListDir, destUrl, key } from './config/local.js';

// Create log file
const logFileName = `${logFileDir}${format(new Date(), "yyyy-MM-dd'T'hh-mm-ss")}.log`;
const logger = createLogger(logFileName);

// try {
// Get the full list of Azure Blob Storage accounts under a given
// subscription ID then get the storage account keys
logger.info(msgLog('Getting Azure Storage account list'));
const storageAccounts = storageAccountList(subscriptionId);
const storageAccountsWithKeys = [];
for await (const storageAccount of storageAccounts) {
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
    const path = `${storageAccount.id}/${container.name}`;
    const base = `${path.replace(/\.|\//g, '_')}.csv`;
    const outputPath = `${fileListDir}${base}`;
    logger.info(msgLog(`Starting file scan on container ${container.name}.`));
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
      if (!fs.existsSync(outputPath)) {
        fs.writeFileSync(outputPath, 'name,account,container,ResourceType,createdOn,lastModified,contentLength,contentMD5,accessTier\r\n');
      }
      fs.appendFileSync(outputPath, `${rows}\r\n`, { encoding: 'utf8' });
    }
    // Copy the local blob listing CSV to blob storage
    const destBlob = `${destUrl}${base}`;
    const res = await createBlobFromLocalPath(destBlob, key, outputPath, {}, () => { });
    if (res === 201) {
      logger.info(msgLog(`Output CSV successfully written to blob storage: ${base}`));
    } else if (res === 200) {
      logger.info(msgLog(`Skipped writting output CSV, file size is equal: ${base}`));
    } else {
      throw new Error("Couldn't write blob to destination");
    }
  }
}
// } catch (err) {
//   logger.error(msgLog(err));
// }
