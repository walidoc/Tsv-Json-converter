const fs = require('fs');
const path = require('path');
const rimraf = require('rimraf');
const util = require('util');
const {
  convert,
} = require('./convert');

const remove = util.promisify(rimraf);
const mkdir = util.promisify(fs.mkdir);

const defaultInputDir = 'input';
const defaultOutputDir = 'result';

const convertFile = async () => {

  const outputDir = path.join(__dirname, '', defaultOutputDir);

  try {
    await remove(outputDir);
    await mkdir(outputDir);
    await convert({
      inputPath: path.join(__dirname, '', defaultInputDir, 'data.tsv'),
      outputPath: path.join(outputDir, 'data.json'),
    });
  } catch (e) {
    console.error(e);
  }
};

convertFile();
