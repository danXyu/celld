// Publish only fork documentation. Never copy the upstream documentation tree.
import { mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const site = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const root = path.resolve(site, '..');
const out = path.join(site, 'src/content/docs/fork');
const repo = 'https://github.com/ewhauser/celld';
const pages = [
  { source: 'docs/fork-builds.md', slug: 'releases', title: 'Release notes', description: 'Fork build history, recovery changes, removed APIs, and rollout requirements.' },
  { source: 'docs/previews.md', slug: 'previews', title: 'Application previews', description: 'Deploy isolated Kubernetes previews and clone approved persisted object state.' },
  { source: 'docs/preview-runtime.md', slug: 'preview-runtime', title: 'Snapshot seeding', description: 'Snapshot consistency, initialization authority, and restore boundaries for seeded previews.' },
  { source: 'docs/telemetry.md', slug: 'metrics', title: 'OTLP metrics', description: 'Node gauges and cell CPU and heap distributions added by the fork.', section: 'Metrics' },
  { source: 'docs/export.md', slug: 'export', title: 'Change export', description: 'User guide: stream cell changes to a warehouse, load them into Snowflake, and keep the copy complete.' },
];
const routes = new Map(pages.map(p => [p.source, p.slug]));
rmSync(out, { recursive: true, force: true });
mkdirSync(out, { recursive: true });
for (const page of pages) {
  let body = readFileSync(path.join(root, page.source), 'utf8');
  if (page.section) {
    const match = body.match(new RegExp(`^## ${page.section}\\n([\\s\\S]*?)(?=^## |$(?![\\s\\S]))`, 'm'));
    if (!match) throw new Error(`Missing ${page.section} section in ${page.source}`);
    body = match[1];
    body = `Added in **v0.6.0-ewhauser.2**. For traces, logs, and general telemetry configuration, see the [upstream telemetry guide](https://celld.dev/docs/telemetry/).\n\n## Enable metrics\n\n\`\`\`sh\nexport CELLD_OTEL=http://collector:4318\nexport OTEL_RESOURCE_ATTRIBUTES=celld.fleet=development\nexport OTEL_METRIC_EXPORT_INTERVAL=60000\n\`\`\`\n\nStart celld with these environment variables and your normal fleet arguments. Metrics are enabled by default with an OTLP collector; set \`OTEL_METRICS_EXPORTER=none\` to disable them while retaining traces and logs. The collector base URL supplies the endpoint; celld appends \`/v1/metrics\`.\n\n## Metrics\n\n${body}`;
  } else {
    if (!/^# .+\n/.test(body)) throw new Error(`Missing title in ${page.source}`);
    body = body.replace(/^# .+\n/, '');
  }
  // Preserve code blocks; rewrite Markdown destinations relative to their source.
  let fence = null;
  body = body.split('\n').map(line => {
    const marker = line.match(/^\s*(`{3,}|~{3,})/);
    if (marker) {
      if (!fence) fence = marker[1];
      else if (marker[1][0] === fence[0] && marker[1].length >= fence.length) fence = null;
      return line;
    }
    if (fence) return line;
    return line.replace(/\]\(([^\s)]+)\)/g, (_, href) => {
      if (/^(?:[a-z][a-z\d+.-]*:|#|\/)/i.test(href)) return `](${href})`;
      const [target, fragment] = href.split('#');
      const source = path.posix.normalize(path.posix.join(path.posix.dirname(page.source), target));
      const route = routes.get(source);
      const url = route ? `../${route}/` : `${repo}/blob/main/${source}`;
      return `](${url}${fragment ? `#${fragment}` : ''})`;
    });
  }).join('\n');
  const notice = page.slug === 'export'
    ? ':::caution[On main — not in v0.6.0-ewhauser.2]\nThis page follows development on main. Change export is not in a fork release yet. See the status below before enabling it.\n:::\n\n'
    : '';
  const metadata = { title: page.title, description: page.description, editUrl: `${repo}/edit/main/${page.source}` };
  const frontmatter = Object.entries(metadata).map(([key, value]) => `${key}: ${JSON.stringify(value)}`).join('\n');
  writeFileSync(path.join(out, `${page.slug}.md`), `---\n${frontmatter}\n---\n\n${notice}${body.trim()}\n`);
}
console.log(`Synced ${pages.length} fork pages.`);
