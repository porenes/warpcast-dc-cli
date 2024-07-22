#!/usr/bin/env node

const fs = require("fs");
const csv = require("csv-parser");
const axios = require("axios");
const { createObjectCsvWriter } = require("csv-writer");
const { Command } = require("commander");
const { v4: uuidv4 } = require("uuid");
const cliProgress = require("cli-progress");

const program = new Command();

program
  .version("1.0.0")
  .requiredOption("-f, --file <path>", "input CSV file")
  .requiredOption("-k, --apikey <key>", "API key")
  .option("-o, --output <path>", "output CSV file", "output.csv")
  .parse(process.argv);

const { file, apikey, output } = program.opts();

const csvWriter = createObjectCsvWriter({
  path: output,
  header: [
    { id: "recipientFid", title: "Recipient FID" },
    { id: "message", title: "Message" },
    { id: "idempotencyKey", title: "Idempotency Key" },
    { id: "status", title: "Status" },
    { id: "response", title: "Response" },
  ],
});

let results = [];
let totalRows = 0;

// Create a new progress bar instance
const progressBar = new cliProgress.SingleBar(
  {},
  cliProgress.Presets.shades_classic
);

// Count the total number of rows
function countRows(file) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(file)
      .pipe(csv())
      .on("data", () => {
        totalRows++;
      })
      .on("end", () => {
        resolve(totalRows);
      })
      .on("error", (err) => {
        reject(err);
      });
  });
}

// Process the CSV file
async function processCSV(file, apikey) {
  const stream = fs.createReadStream(file).pipe(csv());
  for await (const row of stream) {
    try {
      const { recipientFid, message } = row;
      const idempotencyKey = uuidv4();
      const response = await callApi(
        recipientFid,
        message,
        idempotencyKey,
        apikey
      );
      results.push({
        recipientFid,
        message,
        idempotencyKey,
        status: "Success",
        response: JSON.stringify(response.data),
      });
    } catch (error) {
      console.error("Error processing row:", row, error.message);
      results.push({
        recipientFid: row.recipientFid,
        message: row.message,
        idempotencyKey: "",
        status: "Error",
        response: error.response
          ? JSON.stringify(error.response.data)
          : error.message,
      });
    } finally {
      // Update the progress bar
      progressBar.increment();
    }
  }
}

// Make the API call
async function callApi(recipientFid, message, idempotencyKey, apiKey) {
  try {
    console.log("Sending request:", { recipientFid, message, idempotencyKey });

    const response = await axios.put(
      "https://api.warpcast.com/v2/ext-send-direct-cast",
      {
        recipientFid,
        message,
        idempotencyKey,
      },
      {
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
      }
    );
    return response;
  } catch (error) {
    if (error.response) {
      console.error("API response error:", error.response.data);
    } else {
      console.error("API request error:", error.message);
    }
    throw error;
  }
}

(async () => {
  try {
    await countRows(file);
    if (totalRows === 0) {
      console.log("No rows found in the CSV file.");
      return;
    }

    // Start the progress bar with the total number of rows
    progressBar.start(totalRows, 0);

    await processCSV(file, apikey);
    await csvWriter.writeRecords(results);
    progressBar.stop();
    console.log("The CSV file was written successfully");
  } catch (error) {
    console.error("Error processing CSV file:", error);
  }
})();
