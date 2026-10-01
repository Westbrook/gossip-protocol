// Read-only cheap gate: parse JavaScript without launching a browser or a server.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = path.resolve(__dirname, '../..');
const locatorPath = path.join(root, '.progress-report/project.json');
const locator = fs.existsSync(locatorPath) ? JSON.parse(fs.readFileSync(locatorPath, 'utf8')) : null;
const files = fs.readdirSync(__dirname).filter(name => name.endsWith('.cjs')).map(name => path.join(__dirname, name));
for (const file of files) new vm.Script(fs.readFileSync(file, 'utf8'), {filename: file});
const htmlFiles = fs.readdirSync(root).filter(name => name.endsWith('.html')).map(name => path.join(root, name));
const reportIndex = locator && path.join(locator.reportWorkspace, 'index.html');
const reportAvailable = Boolean(reportIndex && fs.existsSync(reportIndex));
if (reportAvailable) htmlFiles.push(reportIndex);
let scripts = 0;
for (const file of htmlFiles) {
  for (const match of fs.readFileSync(file, 'utf8').matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script\s*>/gi)) {
    if (/\bsrc\s*=|\btype\s*=\s*["'](?:application\/ld\+json|application\/json)/i.test(match[1])) continue;
    new vm.Script(match[2], {filename: `${file}:inline-${++scripts}`});
  }
}
console.log(JSON.stringify({passed: true, cjsFiles: files.length, htmlFiles: htmlFiles.length, inlineScripts: scripts,
  report: reportAvailable ? 'checked' : 'not present; optional independent workspace'}));
