import yargs from 'yargs'
import { hideBin } from 'yargs/helpers'
import { upload, Arguments } from './uploader';
import workerFarm from 'worker-farm'

// Multipart
async function main(args:Arguments) : Promise<void> {
  return new Promise((resolve, reject) => {
    const workerCount = args.workers;
    const workers = workerFarm({
      maxConcurrentWorkers: workerCount
    }, require.resolve('./uploader'), ['upload'])
    let ret = 0;
    for (var i = 0; i < workerCount; i++) {
      workers.upload(args, function (err:any, outp:any) {
        if (++ret == workerCount)
          workerFarm.end(workers);
          resolve();
      })
    }
  })
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
  .option('workers', {
    alias: 'w',
    type: 'number',
    descript: 'Number of workers to simultaneously generate archives',
    default: 1
  })
  .parseAsync()
  .then(async (yargs) => {
    return await main(yargs);
  })
  .then(() => {
    console.log('Completed!');
  })
  .catch(function(e) { 
    console.error('Failed!', e);
  });

