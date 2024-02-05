'use strict';
const parquet = require('..');

async function example() {
  let reader = await parquet.ParquetReader.openFile('parquet_memory_limit-21mb.parquet');
  let count =0
  let cursor = reader.getCursor();
  let record = null;
  while (record = await cursor.next()) {
    count++
    
  }

  reader.close();
  console.log(count);
}

example();

