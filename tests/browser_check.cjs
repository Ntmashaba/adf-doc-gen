// Dev check: open every generated HTML page in Chromium (Playwright) and
// verify that no view throws, that no page is wider than the window at
// desktop (1400px) and phone (390px) widths with every section expanded,
// and that every link on pages whose name starts with "zz-" navigates
// without error (use it for fixtures with quotes, backslashes, Unicode).
//
//   npm install playwright   (once)
//   node tests/browser_check.cjs <folder of .html files>
const {chromium} = require("playwright");
const fs = require("fs");
const path = require("path");
const D = process.argv[2];
if(!D){ console.error("usage: node tests/browser_check.cjs <folder>"); process.exit(2); }
(async () => {
  const browser = await chromium.launch();
  const errors = [], wide = [];
  let clicks = 0;
  for (const width of [1400, 390]) {
    const page = await browser.newPage({viewport: {width, height: 900}});
    for (const f of fs.readdirSync(D).filter(f => f.endsWith(".html"))) {
      const errs = [];
      page.removeAllListeners("pageerror");
      page.on("pageerror", e => errs.push(e.message));
      await page.goto("file://" + path.resolve(D, f));
      const tabs = await page.evaluate(() => TABS.filter(t => t.avail()).map(t => t.id));
      for (const t of tabs) {
        await page.evaluate(t => switchTab(t), t);
        await page.evaluate(() => document.querySelectorAll("details").forEach(d => d.open = true));
        const w = await page.evaluate(() => document.documentElement.scrollWidth);
        if (w > width + 10) wide.push(`${f} #${t} @${width}px: ${w}px`);
        if (width === 1400 && f.startsWith("zz-")) {
          const n = await page.evaluate(() => document.querySelectorAll("[data-go]").length);
          for (let i = 0; i < n; i++) {
            await page.evaluate(t => switchTab(t), t);
            await page.evaluate(i => { const x = document.querySelectorAll("[data-go]")[i]; if (x) x.click(); }, i);
            clicks++;
          }
        }
      }
      if (errs.length) errors.push(`${f}: ${errs[0]}`);
    }
    await page.close();
  }
  await browser.close();
  console.log(`script errors: ${errors.length}`); errors.slice(0, 10).forEach(e => console.log("  " + e));
  console.log(`too wide: ${wide.length}`); wide.slice(0, 10).forEach(e => console.log("  " + e));
  console.log(`links clicked on zz- pages: ${clicks}`);
  process.exit(errors.length || wide.length ? 1 : 0);
})();
