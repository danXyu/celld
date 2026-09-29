---
title: Contributing
description: Keep fork documentation tied to source and validate the site before merging.
---

Report fork issues and propose changes in
[ewhauser/celld](https://github.com/ewhauser/celld). Follow the
[upstream contribution instructions](https://github.com/denoland/celld#contributions)
for patches intended for upstream.

## Update documentation with the change

Document the affected behavior, the old failure, the new result, and any upgrade
or compatibility boundary. Distinguish released behavior from work on main.
Link the implementation or issue when describing a bug fix.

The site publishes only fork-specific material. Link to
[celld.dev/docs](https://celld.dev/docs/) for the standard runtime and APIs.

The release history, previews, snapshot seeding, export settings, and metrics
reference are generated from the repository's `docs/` files. Their **Edit page**
links point to those source files. Edit those sources, then rebuild the site;
do not edit generated copies.

## Run the site

From the repository root, with Node 24 or newer (below 27) and pnpm:

```sh
cd site
pnpm install --frozen-lockfile
pnpm dev
```

Open `http://localhost:4321/celld/`. To validate the production output:

```sh
pnpm build
pnpm preview
```

The build synchronizes fork sources, creates the static site and search index,
and checks internal links. Check both desktop and mobile navigation before
submitting a layout change. A site build verifies the documentation artifact;
runtime behavior still needs its own tests.

## Publication

GitHub Actions builds the site on pull requests. A push to `main`, including a
merge, builds and publishes it to
[ewhauser.github.io/celld](https://ewhauser.github.io/celld/).
Pull requests upload a reviewable artifact without deploying it.
