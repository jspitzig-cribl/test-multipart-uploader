import {v4 as uuidv4} from 'uuid';
import AWS from 'aws-sdk';
import { UploadPartOutput } from 'aws-sdk/clients/s3';
import { existsSync } from 'fs';
import { readFile, writeFile, lstat, mkdir } from 'fs/promises'
import JSZip from 'jszip'
import { gzip } from 'node-gzip'

import yargs, { command, options } from 'yargs'
import { hideBin } from 'yargs/helpers'
import { join } from 'path/posix';

interface Arguments {
  format: string,
  lines: number,
  fileCount: number,
  output?: string,
  uploadType: string,
  repeat: number
}

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

function chunkArray<T>(array:T[], size:number) : T[][] {
  const result = new Array<Array<T>>(Math.ceil(array.length / size));
  const length = array.length;
  for(let i = 0; i < length; i += size) {
    result.push(array.slice(i, i * size + size))
  }
  return result;
}
async function createLogFile(args:Arguments) : Promise<Buffer> {
  const contents = (await readFile('./source.log')).toString();
  const sourceLines = contents.split('\n');
  const totalSourceLines = sourceLines.length;
  const totalOutputLines = args.lines;
  const repeat = Math.ceil(totalOutputLines/totalSourceLines);
  console.info('Mapping lines');
  const generatedLines = sourceLines.flatMap(l => Array.from({length: repeat}, () => createLine(l)));
  console.info('Building buffer');
  return chunkArray(generatedLines.slice(0, totalOutputLines), 50000)
    .reduce((b, ls) => Buffer.concat([b, Buffer.from(`${ls.join('\n')}\n`)]), Buffer.alloc(0));
}

async function createLogZip(args:Arguments) : Promise<Buffer> {
  const zip = new JSZip();
  for(let i = 0; i < args.fileCount; i++) {
    const fileName = `logs/${uuidv4()}.log`;
    const data = await createLogFile(args);
    zip.file(fileName, data);
  }
  return zip.generateAsync({
    type: 'nodebuffer'
  });
}

async function createLogGZip(args:Arguments) : Promise<Buffer> {
  const data = await createLogFile(args);
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
async function main(args:Arguments) {
  for(let i = 0; (args.repeat < 0) || (i < args.repeat); i++) {
    const uuid = uuidv4();
    console.info('Creating archive');
    let archive:Buffer;
    let contentType:string;
    let fileName:string;
    switch(args.format) {
      case 'gzip':
        archive = await createLogGZip(args);
        contentType = 'application/gzip';
        fileName = `${uuid}.gz`;
        break;
      case 'zip':
        archive = await createLogZip(args);
        contentType = 'application/zip';
        fileName = `${uuid}.zip`;
        break;
      default: throw new Error('Invalid format')
    }
    console.info('Created archive');
    if(args.output && args.output.length > 0) {
      const output = args.output!
      console.info('Writing file');
      let isUsable = existsSync(output);
      if(!isUsable) {
        try {
          const stat = await lstat(output);
          isUsable = stat.isDirectory();
        } catch (e) {}
      }
      if(isUsable) {
        await mkdir(output, {recursive:true});
        await writeFile(`${output}/${uuid}`, archive);
      }
      console.info('Wrote file');
    } else {
      console.info('Uploading archive');
      switch(args.uploadType) {
        case 'multipart':
          await multipartUploadData(archive, fileName, contentType);
          break;
        case 'simple':
          await simpleUploadData(archive, fileName, contentType);
          break;
        default: throw new Error('Invalid upload type')
      }
      console.info('Uploaded archive');
    }
  }
}

yargs(hideBin(process.argv))
  .option('format', {
    alias: 'f',
    type:'string', 
    description: 'Compression format',
    choices: ['gzip', 'zip'], 
    default: 'zip'
  })
  .option('lines', {
    alias: 'l',
    type: 'number',
    description: 'Number of lines per file',
    default: 10000
  })
  .option('fileCount', {
    alias: 'c',
    type: 'number',
    description: 'Number of files per archive (Only applies to zip files)',
    default: 25
  })
  .option('uploadType', {
    alias: 'u',
    type: 'string',
    description: 'Method of uploading to S3.',
    choices: ['multipart', 'simple'],
    default: 'multipart'
  })
  .option('output', {
    alias: 'o',
    type: 'string',
    description: 'Generate out to file only.  Directory to output to.',
  })
  .option('repeat', {
    alias: 'r',
    type: 'number',
    description: 'Number of files to generate. (-1 to generate indefinitely)',
    default: 1
  })
  .parseAsync()
  .then((yargs) => {
    main(yargs);
  })
// ().then(function () {
//   console.log('Completed!');
// }).catch(function(e) {
//   console.error('Failed!', e);
// });
