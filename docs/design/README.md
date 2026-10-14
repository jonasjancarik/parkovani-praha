# Product design references

read_when: changing the React explorer layout, typography, colors, charts, filters, or responsive behavior.

`data-explorer-target.png` is the ImageGen mockup the first React implementation was built from. It still defines the visual language: typography, paper background, colours, and thin rules.

The layout no longer matches it one to one. Since October 2026 the explorer has no map tab, shows permits per space in its own panel under the main chart, puts the metric switch in the chart heading, and compares districts in a single table. The reasons and the full list are in `frontend/AGENTS.md`. The editorial overview from earlier ideation now exists as the homepage; the chart-first explorer (`Vývoj a srovnání`) sits at `/?page=explorer`.

Below the reference-matched first screen, the React explorer expands the original
Streamlit overview into six flat editorial analysis sections. These sections keep
the same visual tokens and shared filters while covering all nine original chart
questions without reproducing the Streamlit page as a long sequence of unrelated
chart blocks.

On wide desktop screens, the shared filters and address search remain visible while
the reader scrolls through the analysis. In compact desktop, tablet, and phone
layouts the toolbar stays in the document flow, with the address search moving below
the filters before the controls can collide or crowd the available chart space.
