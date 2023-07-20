"use strict";
const chai = require("chai");
const path = require("path");
const assert = chai.assert;
const parquet = require("../parquet");
const server = require("./mocks/server");

describe("ParquetReader", () => {
  describe("#openUrl", () => {
    before(() => {
      server.listen();
    });

    afterEach(() => {
      server.resetHandlers();
    });

    after(() => {
      server.close();
    });
  });

  describe("#asyncIterator", () => {
    it("responds to for await", async () => {
      const reader = await parquet.ParquetReader.openFile(
        path.join(__dirname,'test-files','fruits.parquet')
      );

      let counter = 0;
      for await(const record of reader) {
        counter++;
      }

      assert.equal(counter, 40000);
    })
  });

    describe("#handleDecimal", () => {
        it("loads parquet with columns configured as DECIMAL", async () => {
            const reader = await parquet.ParquetReader.openFile(
                path.join(__dirname,'test-files','valid-decimal-columns.parquet')
            );

            const data = []
            for await(const record of reader) {
                data.push(record)
            }

            assert.equal(data.length, 4);
            assert.equal(data[0].over_9_digits, 118.0297106);
            assert.equal(data[1].under_9_digits, 18.7106);
            // handling null values
            assert.equal(data[2].over_9_digits, undefined);
            assert.equal(data[2].under_9_digits, undefined);
        })
    });
});
