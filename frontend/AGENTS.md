# Prototype Instructions

Run the local server yourself and open the preview in the browser available to this environment. Do not give the user server-start instructions when you can run it.

Before making substantial visual changes, use the Product Design plugin's `get-context` skill when the visual source is unclear or no longer matches the current goal. When the user gives durable prototype-specific design feedback, preferences, or decisions, record them in `AGENTS.md`.

When implementing from a selected generated mock, treat that image as the source of truth for layout, component anatomy, density, spacing, color, typography, visible content, and hierarchy.

## Parkování Praha direction

- `../docs/design/data-explorer-target.png` set the visual language (type, paper, colours, rules). The layout has since moved away from it on purpose; the decisions below win where they differ from the image.
- The main analytical surface is the chart-first `Datový průzkumník` with `Grafy` and `Tabulka` as peer views. `Mapa` stays hidden until there is a real map; do not bring back a placeholder tab.
- `Městský přehled` (`src/OverviewPage.jsx`) is the homepage: a few ready answers about the whole city with no filters, each leading into the explorer or `Okolí adresy`. Keep it short and keep its sentences generated from the data. Do not fold it into the explorer screen, and do not move explorer-only analyses (forecast, ZSJ, yearly change distribution) onto it.
- Preserve the editorial serif/sans pairing, warm paper background, Prague red, deep teal, thin rules, direct chart labels, and low-card visual hierarchy.

### Decisions approved by Jonáš on 2026-10-13

- Colour carries meaning: red is parking permits, teal is parking spaces, slate is permits per space. Breakdown charts (zones, permit types, parker groups) use the separate muted palette and never red or slate. Interface chrome stays neutral; red is otherwise only the active tab or switch.
- Selected filter values are quiet outlined chips, not red ones.
- The main chart has no fills under the lines and no second y-axis. Permits per space sits in its own low panel under the counts with a shared time axis.
- The metric choice is a switch in the chart heading, not a select in the page header. The explainer text sits directly under the title.
- District comparison is one table with an inline bar, not a chart beside a table.
- Charts with several similar lines draw them grey, colour only the leading one, and highlight on hover.
- Page content is capped at 1440px. Prefer whitespace to an extra horizontal rule between sections.
