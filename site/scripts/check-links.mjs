// Starlight's validator skips relative URLs. Check the rendered site as a browser
// resolves it, including fragments, generated navigation, and local assets.
import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse } from 'parse5';

const dist = fileURLToPath(new URL('../dist/', import.meta.url));
const base = `/${(process.env.SITE_BASE ?? '/celld').split('/').filter(Boolean).join('/')}`.replace(/\/$/, '') + '/';
const origin = new URL(process.env.SITE_URL ?? 'https://ewhauser.github.io').origin;
const documents = new Map();
function collect(directory) {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const filename = path.join(directory, entry.name);
    if (entry.isDirectory()) collect(filename);
    else if (entry.name.endsWith('.html')) {
      const ids = new Set();
      const links = [];
      function visit(node) {
        for (const attr of node.attrs ?? []) {
          if (attr.name === 'id') ids.add(attr.value);
          if ((attr.name === 'href' || attr.name === 'src') &&
              !node.attrs.some(a => a.name === 'rel' && a.value === 'canonical')) links.push(attr.value);
        }
        for (const child of node.childNodes ?? []) visit(child);
      }
      visit(parse(readFileSync(filename, 'utf8')));
      documents.set(filename, { ids, links });
    }
  }
}
collect(dist);
const errors = [];
for (const [filename, { links }] of documents) {
  const relative = path.relative(dist, filename).split(path.sep).join('/');
  const pageUrl = new URL(base + relative.replace(/index\.html$/, ''), origin);
  for (const href of links) {
    const target = new URL(href, pageUrl);
    if (target.origin !== origin) continue;
    if (!target.pathname.startsWith(base)) {
      // Explicit links to sibling GitHub Pages projects are external.
      if (/^https?:\/\//.test(href)) continue;
      errors.push(`${relative}: URL escapes site base: ${href}`);
      continue;
    }
    let local = path.resolve(dist, decodeURIComponent(target.pathname.slice(base.length)));
    if (local !== dist.replace(/\/$/, '') && !local.startsWith(dist)) {
      errors.push(`${relative}: URL escapes output directory: ${href}`);
      continue;
    }
    if (existsSync(local) && statSync(local).isDirectory()) local = path.join(local, 'index.html');
    if (!existsSync(local)) errors.push(`${relative}: missing target: ${href}`);
    else if (target.hash && documents.has(local) && !documents.get(local).ids.has(decodeURIComponent(target.hash.slice(1)))) {
      errors.push(`${relative}: missing anchor: ${href}`);
    }
  }
}
if (errors.length) throw new Error(`Broken site links:\n${[...new Set(errors)].join('\n')}`);
console.log(`Checked pages, anchors, and assets in ${documents.size} HTML files under ${base}.`);
