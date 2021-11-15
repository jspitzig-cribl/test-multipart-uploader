import workerFarm from 'worker-farm'
import { parseArgs,Arguments } from './args';

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


parseArgs()
  .then(main)
  .then(() => {
    console.log('Completed!');
  })
  .catch(function(e) { 
    console.error('Failed!', e);
  });