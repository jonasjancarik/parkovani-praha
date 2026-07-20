# Design QA — React Data Explorer

## Comparison setup

- Reference: `docs/design/data-explorer-target.png`
- Implementation capture: `docs/design/implementation-sticky-final.png` (local QA artifact, ignored by Git)
- Normalized capture: `docs/design/implementation-sticky-normalized.png` (local QA artifact, ignored by Git)
- Combined comparison: `docs/design/comparison-sticky-final.png` (local QA artifact, ignored by Git)
- Viewport and state: 1487 × 1058, Grafy active, all districts/time periods/zone types selected

## Fidelity review

- Layout and spacing: the masthead, three-column introduction, tabs, filter row, chart canvas, district comparison, and insight rail align with the reference hierarchy and proportions. The filter row and address search remain one continuous 98px band both before and after it becomes sticky.
- Typography: editorial serif display type and compact sans-serif controls preserve the reference contrast, scale, line height, and information density.
- Color and surfaces: warm paper background, charcoal text, red active states, teal capacity series, slate ratio series, hairline rules, and low-radius inputs match the source. The main lines now carry a subtle user-requested tonal gradient and low-opacity under-fill; there is no elevated card stack, CSS illustration, or fake imagery.
- Charts: line weights, dashed grid, dual axes, direct end labels, district lollipop chart, and five-row comparison table match the reference treatment. Values, curve shapes, rankings, and the latest date intentionally come from the repository's live dataset instead of the mockup's illustrative numbers.
- Extended analysis: the below-fold continuation uses the same flat editorial system and covers all nine original Streamlit overview questions through six coherent surfaces: city trend, district pressure, detailed type trends, capacity and parker composition, annual change and its distribution, ZSJ pressure, and district forecast.
- Icons: calendar, search, close, disclosure, and information marks use one consistent Tabler stroke family and align to their controls.
- Copy: headings, labels, selected-filter summary, metrics, note, loading state, error state, map prompt, and table headings are complete Czech interface copy.

## Behavior and resilience

- Verified Grafy, Mapa, and Tabulka state changes.
- Verified at 1487 × 1058 that the full filter and address-search band pins at `top: 0` after scrolling 1,450px, without horizontal overflow or overlap between controls.
- Verified the main metric selector, detailed trend modes, annual-change scatter/histogram switch, and forecast district selector.
- Verified the shared district selector updates and resets the headline charts, insight summary, compositions, local changes, pressure series, and forecast data.
- Verified 1024 × 900 with no filter overlap or horizontal overflow.
- Verified 390 × 844 with a static single-column filter band, compact chart legends, hidden direct end labels, five district rows, stacked insight and analysis content, readable compositions and pressure charts, and no horizontal overflow. At 1,200px scroll the toolbar is out of view rather than consuming the phone viewport.
- Semantic headings, tabs, labels, buttons, table structure, status/error regions, keyboard focus indicators, and reduced animation scope are present.

## Findings

- P0: none.
- P1: none.
- P2: none.

## Comparison history

- Sticky-toolbar pass: moving the address search into the filter band preserved the source composition at the comparison viewport. The combined full-view comparison and focused filter-band inspection found no new P0/P1/P2 drift; the new scrolled state keeps that same band intact at the top of the viewport.

final result: passed
