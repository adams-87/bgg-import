import fs from "fs";
import fetch from "node-fetch";
import unzipper from "unzipper";
import parse from "csv-parse/lib/sync";

const ZIP_URL = "https://boardgamegeek.com/filepage/267222/bg_ranks_csvzip"; // URL wymaga zalogowania – do testów plik lokalny
const CSV_FILE = "boardgames_ranks.csv";
const OUTPUT_FILE = "top1000_ids.json";

async function downloadAndExtractCSV() {
  const zipPath = "bg_ranks.zip";
  const res = await fetch(ZIP_URL);
  const fileStream = fs.createWriteStream(zipPath);
  await new Promise((resolve, reject) => {
    res.body.pipe(fileStream);
    res.body.on("error", reject);
    fileStream.on("finish", resolve);
  });

  await fs.createReadStream(zipPath)
    .pipe(unzipper.ParseOne())
    .pipe(fs.createWriteStream(CSV_FILE));

  fs.unlinkSync(zipPath);
}

function parseTop1000IDs() {
  const csvData = fs.readFileSync(CSV_FILE, "utf8");
  const records = parse(csvData, {
    columns: true,
    skip_empty_lines: true,
  });

  const filtered = records
    .filter(r => r["Board Game Rank"] && !isNaN(Number(r["Board Game Rank"])))
    .sort((a, b) => Number(a["Board Game Rank"]) - Number(b["Board Game Rank"]))
    .slice(0, 1000)
    .map(r => Number(r.objectid));

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(filtered, null, 2), "utf8");
  console.log(`Zapisano ${filtered.length} ID do ${OUTPUT_FILE}`);
}

(async () => {
  await downloadAndExtractCSV();
  parseTop1000IDs();
})();
