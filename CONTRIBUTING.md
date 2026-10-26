# Contributing

Thanks for helping. Three kinds of change are most useful:

- **Corrections.** A number that is off by an order of magnitude, a race the design misses, a failure case
  with no recovery. Open an issue or a pull request with the reasoning.
- **Better diagrams.** Edit the `*.diagram.json` file, re-render, and commit the spec and both PNGs.
- **New designs.** Open an issue first with the system and category, so two people do not write the same
  one.

## Adding a design

1. Copy [template/DESIGN_TEMPLATE.md](template/DESIGN_TEMPLATE.md) to
   `designs/<category>/<system>/README.md`. Use an existing category folder if one fits.
2. Fill every section. Numbers need their arithmetic. Tables need keys, indexes and the constraint that
   protects the design's invariant.
3. Add at least an architecture diagram and one sequence diagram under `diagrams/`
   ([format](tools/DIAGRAMS.md)), render them, and look at both PNGs before committing.
4. Add a row to the table in the root [README](README.md).
5. Run `node tools/render-diagrams.mjs --check` and `node tools/check-docs.mjs`.

## Writing style

- Write like you are explaining at a whiteboard: short sentences, concrete numbers, honest limits.
- State staleness precisely: the window, the safe direction of error, where the truth is enforced.
- Every cache key says what happens if it is lost. Every external call says what happens on a timeout.
- Prefer one clear option with its cost over a list of five options with none.
- No marketing words, no emoji.
