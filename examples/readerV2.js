'use strict';
const parquet = require('..');

async function readerV2(){

    let reader = await parquet.ParquetReader.openFile('parquet_memory_limit-21mb.parquet')
    let cursor = reader.getCursor();
    let count = 0
    for(let index=0; index<reader.envelopeReader.metadata.row_groups.length;index++){
      console.log({index})
        for await (const _row of cursor.nextByRowGroups(index)) {
            console.log({
                _row
            })
            count++
     }

    }
    console.log(count);

  }
readerV2()  