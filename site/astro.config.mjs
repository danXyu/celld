// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';

const repo = 'https://github.com/ewhauser/celld';
export default defineConfig({
  site: process.env.SITE_URL ?? 'https://ewhauser.github.io',
  base: process.env.SITE_BASE ?? '/celld',
  trailingSlash: 'always',
  integrations: [starlight({
    title: 'celld fork',
    description: 'Features, fixes, and release notes for the ewhauser/celld fork.',
    favicon: '/favicon.svg',
    social: [{ icon: 'github', label: 'GitHub', href: repo }],
    editLink: { baseUrl: `${repo}/edit/main/site/` },
    customCss: ['./src/styles/custom.css'],
    components: {
      SiteTitle: './src/components/SiteTitle.astro',
      Footer: './src/components/Footer.astro',
    },
    plugins: [starlightLinksValidator({ errorOnRelativeLinks: false, errorOnLocalLinks: false })],
    sidebar: [
      { label: 'The fork', items: [
        { label: 'Overview', slug: '' },
        { label: 'Install and upgrade', slug: 'install' },
        { label: 'Bug fixes', slug: 'fixes' },
        { label: 'Release notes', slug: 'fork/releases' },
      ] },
      { label: 'Features', items: [
        { label: 'Application previews', slug: 'fork/previews' },
        { label: 'OTLP metrics', slug: 'fork/metrics' },
        { label: 'Change export', slug: 'fork/export', badge: { text: 'In progress', variant: 'caution' } },
      ] },
      { label: 'Upstream documentation ↗', link: 'https://celld.dev/docs/' },
      { label: 'Related projects', items: [
        { label: 'Kubernetes operator ↗', link: 'https://ewhauser.github.io/celld-operator/' },
        { label: 'Compatibility results ↗', link: 'https://ewhauser.github.io/celld-tck/' },
        { label: 'Upstream source ↗', link: 'https://github.com/denoland/celld' },
      ] },
      { label: 'Contributing', slug: 'contributing' },
    ],
  })],
});
