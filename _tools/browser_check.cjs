/* Real browser checks use an existing pinned Playwright install; no installation. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const [moduleRoot, base, output] = process.argv.slice(2);
if (!moduleRoot || !base || !output) throw new Error('Usage: node browser_check.cjs PLAYWRIGHT_MODULE BASE_URL NEW_RECEIPT.json');
if (fs.existsSync(output)) throw new Error('Receipt must be a new file');
const pkg = require(path.join(moduleRoot, 'package.json'));
assert.equal(pkg.version, '1.62.1', 'Pinned Playwright version');
const {chromium} = require(moduleRoot);
(async () => {
  const browser = await chromium.launch({headless:true});
  const observations = [];
  const errors = [];
  const requests = [];
  let publicationSha256;
  const pages = ['index.html','outputs/index.html','outputs/benchmark-comparison.html?progress-report','docs/index.html','docs/large-swarm-project-plan-v1.html','docs/peer-library-project-v4.html','progress/archive.html'];
  try {
    for (const viewport of [{width:1440,height:1000},{width:390,height:844}]) {
      const context = await browser.newContext({viewport,serviceWorkers:'block',acceptDownloads:false});
      await context.route('**/*',route=>new URL(route.request().url()).origin===new URL(base).origin ? route.continue() : route.abort());
      const page = await context.newPage();
      page.on('pageerror',e=>errors.push(String(e)));
      page.on('request',r=>requests.push({method:r.method(),url:r.url()}));
      const manifestResponse = await context.request.get(new URL('publication.json',base).href);
      assert.equal(manifestResponse.status(),200);
      const currentManifestSha256 = crypto.createHash('sha256').update(await manifestResponse.body()).digest('hex');
      if(publicationSha256) assert.equal(currentManifestSha256,publicationSha256,'Publication did not change during browser verification');
      publicationSha256 = currentManifestSha256;
      for (const route of pages) {
        const response = await page.goto(new URL(route,base).href);
        assert.equal(response.status(),200,route);
        await page.locator('h1').waitFor();
        assert.equal(await page.locator('form,textarea,[data-review],[data-feedback]').count(),0,'No write controls');
        const horizontal = await page.evaluate(()=>({body:document.body.scrollWidth,viewport:innerWidth}));
        assert(horizontal.body <= horizontal.viewport + 1,route+' has horizontal body overflow');
        if (route === 'index.html') {
          await page.getByText('Earlier published checkpoints',{exact:true}).click();
          assert(await page.locator('details[open]').count()>=1);
          await page.getByText('Published snapshot, not a live monitor.',{exact:true}).waitFor();
          await page.screenshot({path:output.replace(/\.json$/,`-${viewport.width}.png`),fullPage:false});
        }
        if (route.includes('?progress-report')) {
          const back = page.locator('a.return');
          await back.waitFor({state:'visible'});
          await back.click();
          assert.equal(new URL(page.url()).pathname,new URL('index.html',base).pathname);
        }
        observations.push({route,viewport,status:'passed'});
      }
      await page.goto(new URL('outputs/benchmark-comparison.html',base).href);
      assert.equal(await page.locator('a.return').isVisible(),false,'Return hidden without flag');
      await context.close();
    }
    assert.deepEqual(errors,[],'No browser errors');
    assert(!requests.some(r=>r.method!=='GET'||new URL(r.url).pathname.includes('/api/')),'Read-only no API traffic');
    const receipt={protocol:'gossip-pages-browser-check-v1',status:'passed',physically_executed:true,playwright:pkg.version,chromium:browser.version(),publication_sha256:publicationSha256,base_url:base,observations,requests:requests.length,non_get_requests:0,api_requests:0,browser_errors:errors};
    fs.writeFileSync(output,JSON.stringify(receipt,null,2)+'\n',{flag:'wx'});
    console.log(JSON.stringify(receipt));
  } finally {await browser.close();}
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
