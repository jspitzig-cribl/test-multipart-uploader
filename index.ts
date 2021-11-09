import {v4 as uuidv4} from 'uuid';
import AWS from 'aws-sdk';
import { UploadPartOutput } from 'aws-sdk/clients/s3';
import { readFile, writeFile } from 'fs/promises'
import JSZip from 'jszip'
import { gzip } from 'node-gzip'

const s3 = new AWS.S3();

async function uploadPart(s3:AWS.S3, partParams:AWS.S3.UploadPartRequest):Promise<UploadPartOutput> {
  let error;
  let tryNumber = 0;
  do {
    try {
      error = null;
      return await s3.uploadPart(partParams).promise()
    } catch(e) {
      error = e;
      tryNumber++;
      console.error(`Upload part request failed.  Retrying.  Retry #${tryNumber}`)
    }
  } while(true);
}
function createLine(template:string) : string {
  return template
      .replace(':::replace:::', `:::${uuidv4()}:::${uuidv4()}:::${uuidv4()}:::${uuidv4()}:::`)
      .replace(':::time:::', new Date().toISOString())
}
async function createLogFile(repeat:number = 100) : Promise<Buffer> {
  const contents = (await readFile('./source.log')).toString();
  const lines = contents.split('\n')
    .flatMap(l => Array.from({length: repeat}, () => createLine(l)));
  return Buffer.from(lines.join('\n'))
}

async function createLogZip(numFiles:number) : Promise<Buffer> {
  const zip = new JSZip();
  for(let i = 0; i < numFiles; i++) {
    const fileName = `logs/${uuidv4()}.log`;
    const data = await createLogFile();
    zip.file(fileName, data);
  }
  return zip.generateAsync({
    type: 'nodebuffer'
  });
}

async function createLogGZip() : Promise<Buffer> {
  const data = await createLogFile(2500);
  return await gzip(data);
}

async function getChunk(data:Buffer, chunkSize:number, chunk:number) {
  const start = chunk * chunkSize;
  const end = start + chunkSize;
  if(end >= data.byteLength - 1) {
    return data.slice(start);
  } else {
    return data.slice(start, end)
  }
}

async function multipartUploadData(data:Buffer, id:string, ContentType:string) : Promise<void> {
  const Bucket = 'jspitzig-cribl-test';
  const Key = `multipart-test/${id}`;
  const multiPartParams = { Bucket, Key, ContentType };  
  console.info('Creating multipart upload');
  const multipart = await s3.createMultipartUpload(multiPartParams).promise();
  console.info('Created multipart upload');
  const MultipartUpload : AWS.S3.CompletedMultipartUpload = { Parts: [] };
  const UploadId = multipart.UploadId!
  const chunkSize = 5*1024*1024;
  const chunkCount = Math.ceil(data.byteLength / chunkSize)
  for(let PartNumber = 1; PartNumber <= chunkCount; PartNumber++) {
    const Body = await getChunk(data, chunkSize, PartNumber - 1);
    const partParams :AWS.S3.UploadPartRequest = { Body, Bucket, Key, PartNumber, UploadId, ContentLength:Body.byteLength};
    console.info(`Uploading part #${PartNumber} of size ${Body.byteLength}`);
    const uploadResult = await uploadPart(s3, partParams);
    console.info(`Uploaded part #${PartNumber}`);
    const ETag = uploadResult.ETag!
    MultipartUpload.Parts![PartNumber - 1] = { PartNumber, ETag };
  }
  const doneParams:AWS.S3.CompleteMultipartUploadRequest = 
    { Bucket, Key, UploadId, MultipartUpload };
  console.info(`Completing upload`);
  await s3.completeMultipartUpload(doneParams).promise();
  console.info(`Completed upload`);
}

async function simpleUploadData(data:Buffer, id:string, ContentType:string) : Promise<void> {
  const Bucket = 'jspitzig-cribl-test';
  const Key = `multipart-test/${id}`;
  await s3.upload({
    Bucket,
    Key,
    ContentType,
    ContentLength:data.byteLength,
    Body: data
  }).promise()

}

// Multipart
(async function main() {
  const uuid = uuidv4();
  console.info('Creating zip');
  //const zip = await createLogZip(25);
  const zip = await createLogGZip();
  console.info('Created zip');
  console.info('Uploading zip');
  //const fileName = `${uuid}.zip`;
  const fileName = `${uuid}.gz`;
  //const contentType = 'application/zip';
  const contentType = 'application/gzip';
  await simpleUploadData(zip, fileName, contentType);
  // await multipartUploadData(zip, fileName, contentType);
  console.info('Uploaded zip');
})().then(function () {
  console.log('Completed!');
}).catch(function(e) {
  console.error('Failed!', e);
});
