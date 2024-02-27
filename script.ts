import * as fs from "fs";
import * as readline from "readline";
import * as util from "util";
import * as stream from "stream";

const lineByLine = require("n-readlines"); // old module (for read files line by line)

// Outer Sort because we have only 500mb dynamic memory
// This algorithm can also be improved by using a multi-path method, i.e. more files

// 1tb static memory
// 500mb dynamic memory
// 1 str < 1_000_000 chars

// function when stream finish
const finished = util.promisify(stream.finished);

// split to files by chunks
async function SplitToFiles(filename: string, chunk_size: number) {
  // init streams
  const writeStreamLeft = fs.createWriteStream("left.txt");
  const writeStreamRight = fs.createWriteStream("right.txt");
  const readStream = fs.createReadStream(filename);
  const rl = readline.createInterface({
    input: readStream,
    crlfDelay: Infinity,
  });
  //

  // flag to understand which file the recording will go to
  let flag = true;

  // count lines
  let counter = 0;

  // count segments
  let segments = 1;

  // distribution of lines across files
  for await (const line of rl) {
    if (counter == chunk_size) {
      flag = !flag;
      counter = 0;
      segments++;
    }

    if (flag) {
      writeStreamLeft.write(line + "\n");
    } else {
      writeStreamRight.write(line + "\n");
    }

    counter++;
  }
  //

  // wait end of the streams
  writeStreamLeft.end();
  writeStreamRight.end();
  await finished(writeStreamLeft);
  await finished(writeStreamRight);
  //

  return segments;
}

// take chunk in dynamic memory, sort and write to file
async function SplitToFilesWithSort(filename: string, chunk_size: number) {
  // init streams
  const writeStreamLeft = fs.createWriteStream("left.txt");
  const writeStreamRight = fs.createWriteStream("right.txt");
  const readStream = fs.createReadStream(filename);
  const rl = readline.createInterface({
    input: readStream,
    crlfDelay: Infinity,
  });
  //

  // flag to understand which file the recording will go to
  let flag = true;

  // array of lines
  let chunk: string[] = [];

  // count segments
  let segments = 1;

  // collect lines, then sort and write in file
  for await (const line of rl) {
    chunk.push(line);
    if (chunk.length >= chunk_size) {
      chunk.sort();
      if (flag) {
        writeStreamLeft.write(chunk.join("\n") + "\n");
      } else {
        writeStreamRight.write(chunk.join("\n") + "\n");
      }
      flag = !flag;
      chunk = [];
      segments++;
    }
  }
  //

  // if there are unfinished lines
  if (chunk.length > 0) {
    chunk.sort();
    if (flag) {
      writeStreamLeft.write(chunk.join("\n") + "\n");
    } else {
      writeStreamRight.write(chunk.join("\n") + "\n");
    }
  }
  //

  // wait end of the streams
  writeStreamLeft.end();
  writeStreamRight.end();
  await finished(writeStreamLeft);
  await finished(writeStreamRight);
  //

  return segments;
}

// merge files
async function MergeFiles(filename: string, chunk_size: number) {
  // init streams
  const writeStreamFile = fs.createWriteStream(filename);
  const readfileStreamLeft = new lineByLine("left.txt");
  const readfileStreamRight = new lineByLine("right.txt");
  //

  // take first element of files
  let leftElement = readfileStreamLeft.next();
  let rightElement = readfileStreamRight.next();
  //

  // count chunks
  let leftCounter = 0;
  let rightCounter = 0;
  //

  // merge
  while (leftElement || rightElement) {
    let currentRecord;
    let flag = false;

    let leftLineString = leftElement.toString() + "\n";
    let rightLineString = rightElement.toString() + "\n";

    // if no element left (or element right) or chunk ended. else check both lines
    if (!leftElement || leftCounter == chunk_size) {
      currentRecord = rightLineString;
    } else if (!rightElement || rightCounter == chunk_size) {
      currentRecord = leftLineString;
      flag = true;
    } else {
      // A < a (check on that)
      if (leftLineString.toLowerCase() < rightLineString.toLowerCase()) {
        currentRecord = leftLineString;
        flag = true;
      } else {
        currentRecord = rightLineString;
      }
    }

    // write the reslting string
    writeStreamFile.write(currentRecord);

    // next line
    if (flag) {
      leftElement = readfileStreamLeft.next();
      leftCounter++;
    } else {
      rightElement = readfileStreamRight.next();
      rightCounter++;
    }

    // if both chunk ended counters = 0
    if (leftCounter != chunk_size || rightCounter != chunk_size) {
      continue;
    }

    leftCounter = 0;
    rightCounter = 0;
  }
  //

  // wait end of the stream
  writeStreamFile.end();
  await finished(writeStreamFile);
  //
}

async function OuterSort(filename: string, chunk_size: number) {
  // first split with sort chunks
  let segments = await SplitToFilesWithSort(filename, chunk_size);
  await MergeFiles(filename, chunk_size);
  chunk_size *= 2;
  //

  // split and merge
  while (true) {
    let segments = await SplitToFiles(filename, chunk_size);

    if (segments == 1) break;

    await MergeFiles(filename, chunk_size);
    chunk_size *= 2;
  }
  //

  // delete help files
  fs.unlink("left.txt", () => {});
  fs.unlink("right.txt", () => {});
  //
}

// test
(async () => {
  const testStream = fs.createWriteStream("input.txt");

  const linesQuntity = 5000;
  const charsQuntity = 1000;

  function generateRandomString(length: number) {
    let result = "";
    let characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (let i = 0; i < length; i++) {
      result += characters.charAt(
        Math.floor(Math.random() * characters.length)
      );
    }
    return result;
  }

  for (let i = 0; i < linesQuntity; i++) {
    let lengthOfString = Math.random() * charsQuntity + 1;
    testStream.write(generateRandomString(lengthOfString) + "\n");
  }

  testStream.end();

  await finished(testStream);

  // if we have 500 Mb dm
  // we can take approximately 400 lines
  OuterSort("input.txt", 400);
})();
