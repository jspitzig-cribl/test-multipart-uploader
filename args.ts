import yargs from "yargs";
import { hideBin } from "yargs/helpers";

export interface Arguments {
  format: string,
  lines: number,
  fileCount: number,
  output?: string,
  uploadType: string,
  repeat: number,
  workers: number,
  cleanupDelay?: string
}

export async function parseArgs():Promise<Arguments> {
  return yargs(hideBin(process.argv))
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
    .option('cleanupDelay', {
      alias: 'd',
      type: 'string',
      description: 'Clean up objects after a delay.'
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
}