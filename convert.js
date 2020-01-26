const parse = require('csv-parse');
const fs = require('fs');
const transform = require('stream-transform');
const util = require('util');

const writeFile = util.promisify(fs.writeFile);
const appendFile = util.promisify(fs.appendFile);

module.exports.convert = ({outputPath, inputPath, shouldLogProgress = true}) => new Promise(
    async (resolve, reject) => {
      try {
        // create empty output file. Otherwise, we wont' be able to create a writable
        // stream for output:
        await writeFile(outputPath, '', 'utf8');

        const source = fs.createReadStream(inputPath, 'utf8');
        const destination = fs.createWriteStream(outputPath, 'utf8');

        const parser = parse({
          // Because the input is tab-delimited:
          delimiter: '\t',
          // Because we want the library to automatically associate the column name
          // with column value in each row for us:
          columns: true,
          // Because we don't want accidental quotes inside a column to be
          // interpreted as "wrapper" for that column content:
          quote: false,
        });

        let outputIndex = 0;
        const transformer = transform((rawRow) => {
          const currentRecordIndex = outputIndex;
          outputIndex += 1;
          if (outputIndex % 100000 === 0 && shouldLogProgress === true) {
            console.info('processing row ', outputIndex);
          }
          const {isAdult, startYear, endYear, runtimeMinutes, genres, ...rest} = rawRow;
          const parsedRow = {
            ...rest,
            isAdult: !!(isAdult === '1'),
            startYear: parseInt(startYear, 10),
            endYear: (endYear === '\N') ? null : parseInt(endYear, 10),
            runtimeMinutes: (runtimeMinutes === '\N') ? null : parseInt(runtimeMinutes, 10),
            genres: genres.split(','),
          };
          const result = (currentRecordIndex === 0) ? `[${JSON.stringify(parsedRow)}` : `,${JSON.stringify(parsedRow)}`;
          return result;
        });

        destination.on('finish', async () => {
          if (outputIndex === 0) {
            await appendFile(outputPath, '[]', 'utf8');
          } else {
            await appendFile(outputPath, ']', 'utf8');
          }
          resolve();
        });

        source.pipe(parser).pipe(transformer).pipe(destination);

      } catch (e) {
        reject(e);
      }
    },
  );
