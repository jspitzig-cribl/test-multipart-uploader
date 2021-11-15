import {v4 as uuidv4} from 'uuid';
import AWS from 'aws-sdk';
import { existsSync } from 'fs';
import { readFile, writeFile, lstat, mkdir } from 'fs/promises'
import JSZip from 'jszip'

import { Readable } from 'stream';
import { createGzip } from 'zlib';

export interface Arguments {
  format: string,
  lines: number,
  fileCount: number,
  output?: string,
  uploadType: string,
  repeat: number,
  workers: number
}

const s3 = new AWS.S3();

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
async function createLogFile(args:Arguments) : Promise<Readable> {
  const contents = (await readFile('./source.log')).toString();
  const sourceLines = contents.split('\n');
  const totalSourceLines = sourceLines.length;
  const totalOutputLines = args.lines;
  const repeat = Math.ceil(totalOutputLines/totalSourceLines);
  console.info('Mapping lines');
  const generatedLines = sourceLines.flatMap(l => Array.from({length: repeat}, () => createLine(l)));
  console.info('Building buffer');
  return Readable.from(generatedLines.slice(0, totalOutputLines))
}

async function createLogZip(args:Arguments) : Promise<Readable> {
  const zip = new JSZip();
  for(let i = 0; i < args.fileCount; i++) {
    const fileName = `logs/${uuidv4()}.log`;
    const data = await createLogFile(args);
    zip.file(fileName, data);
  }
  return new Readable().wrap(zip.generateNodeStream());
}

async function createLogGZip(args:Arguments) : Promise<Readable> {
  return (await createLogFile(args)).pipe(createGzip());
}

async function uploadData(data:Readable, id:string, ContentType:string, partSize:number = 5*1024*1024) : Promise<void> {
  const Bucket = 'jspitzig-cribl-test';
  const Key = `multipart-test/${id}`;
  await s3.upload({
    Bucket,
    Key,
    ContentType,
    Body: data,
  }, {queueSize: 1, partSize}).promise()
}

// Multipart
export async function upload(args:Arguments, resolve:(error?:Error, result?:any)=>any) {
  for(let i = 0; (args.repeat < 0) || (i < args.repeat); i++) {
    const uuid = uuidv4();
    console.info('Creating archive');
    let archive:Readable;
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
          await uploadData(archive, fileName, contentType);
          break;
        case 'simple':
          // If "partSize" is big enough, the upload should be done without multipart
          await uploadData(archive, fileName, contentType, args.lines * 1000);
          break;
        default: throw new Error('Invalid upload type')
      }
      console.info('Uploaded archive');
    }
  }
  resolve(undefined, true);
}
