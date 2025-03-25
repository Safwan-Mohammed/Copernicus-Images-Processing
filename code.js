const fs = require('fs').promises;
const path = require('path');

const outputDir = path.join(__dirname, 'ProcessedJSON');

const ensureOutputDir = async () => {
  try {
    await fs.access(outputDir);
  } catch (err) {
    await fs.mkdir(outputDir);
  }
};

const processFile = async (fileName) => {
  console.log(`Processing: ${fileName}`);

  try {
    const data = await fs.readFile(fileName, 'utf-8');

    if (fileName.endsWith('.json')) {
      const jsonData = JSON.parse(data);
      const filteredData = jsonData.filter((ele) => {
        const crop = ele.Cropname.toLowerCase();
        return crop.includes("ragi");
      });

      const outputFileName = `Process_${path.basename(fileName)}`;
      const outputFilePath = path.join(outputDir, outputFileName);

      await fs.writeFile(outputFilePath, JSON.stringify(filteredData, null, 2));
      console.log(`Filtered data written to: ${outputFilePath}`);
    } else {
      console.log(`Skipping non-JSON file: ${fileName}`);
    }
  } catch (err) {
    console.error(`Error processing ${fileName}:`, err);
  }
};

const processFilesSequentially = async () => {
  await ensureOutputDir();
  try {
    const files = await fs.readdir(__dirname);

    for (const file of files) {
      const filePath = path.join(__dirname, file);
      const stats = await fs.stat(filePath);

      if (stats.isFile()) {
        await processFile(filePath);
      }
    }
  } catch (err) {
    console.error('Error reading directory:', err);
  }
};

processFilesSequentially();