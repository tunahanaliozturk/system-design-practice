# Diagram specs

Every diagram in this repo is a small JSON file named `<name>.diagram.json`, kept in the `diagrams/`
folder of the design it belongs to. `tools/render-diagrams.mjs` turns each one into two PNGs next to it,
`<name>-light.png` and `<name>-dark.png`, using headless Chrome. There is nothing to install beyond Node 20+
and Chrome or Edge.

```sh
node tools/render-diagrams.mjs                       # render everything under designs/
node tools/render-diagrams.mjs path/to/x.diagram.json
node tools/render-diagrams.mjs --check               # validate specs only, no browser needed
```

Set `CHROME` to the browser binary if the script cannot find it.

Embed both images in Markdown with a `<picture>` element so GitHub picks the one matching the reader's
theme:

```html
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="diagrams/architecture-dark.png">
  <img alt="Short description of what the diagram shows" src="diagrams/architecture-light.png">
</picture>
```

Text anywhere in a spec can contain `` `code` `` spans; they render in a monospace font.

## Architecture diagrams

Boxes on a grid, joined by arrows.

```json
{
  "type": "architecture",
  "title": "URL shortener: high-level architecture",
  "subtitle": "optional one-line takeaway",
  "grid": { "nodeW": 200, "cellW": 290, "rowH": 150 },
  "nodes": [
    { "id": "api", "kind": "service", "label": "Shortener API", "col": 1, "row": 0, "lines": ["POST /links"] }
  ],
  "edges": [
    { "from": "client", "to": "api", "label": "HTTPS", "step": 1 }
  ],
  "groups": [
    { "label": "REGION EU-WEST", "nodes": ["api", "pg"] }
  ]
}
```

### Nodes

| Field | Meaning |
|---|---|
| `id` | Unique id used by edges and groups |
| `kind` | Sets the colour and the small tag above the label: `client`, `gateway`, `service`, `worker`, `postgres`, `nosql`, `redis`, `queue` (Kafka), `search` (Elasticsearch), `storage` (S3 and friends), `cdn`, `external` (dashed border) |
| `tag` | Overrides the tag text, for example `"CASSANDRA"` on a `nosql` node or `"STATE"` for a state machine |
| `label` | Box title |
| `lines` | Up to four short detail lines; keep each under about 26 characters |
| `col`, `row` | Grid position, fractions allowed (`1.5` centres between two cells) |
| `w`, `h` | Optional size override |

Default geometry: a column is 290 px wide and a node 200 px, so there is a 90 px gap between columns.
Column `c` has its left edge at `x = 32 + 290 * c`, its right edge at `x = 232 + 290 * c`. That matters
when you set `via` on an edge.

### Edges

| Field | Meaning |
|---|---|
| `from`, `to` | Node ids |
| `label` | Text in a pill on the line; keep it short, the gap between columns is 90 px |
| `step` | Number in a blue badge, for numbered request flows |
| `dashed` | Async: an event, a message, CDC. Adds a legend at the bottom |
| `both` | Arrowheads on both ends |
| `route` | `auto` (default), `h`, `v`, `hv`, `vh`, `hvh`, `vhv`; see below |
| `via` | For `hvh`: the x of the vertical segment. For `vhv`: the y of the horizontal segment |
| `offset`, `offset2` | Shift the start (and end) of the line in px so parallel lines do not overlap |
| `labelPos` | 0 to 1 along the path (default 0.5) |
| `labelSeg` | Put the label on segment N of the path instead (0 = first), `labelPos` then works within it |
| `labelDx`, `labelDy` | Nudge the label |

Routing: `auto` draws a straight horizontal line when the boxes overlap vertically, a straight vertical
line when they overlap horizontally, and an elbow otherwise. Elbows cut through any box in their way, so
for anything not in the same row or column prefer `hvh` with a `via` inside the gap between columns.
A fan-out from one box to a column of boxes reads well as several `hvh` edges sharing one `via`, with
`"labelSeg": 1` so each label sits on its own vertical stretch.

## Sequence diagrams

```json
{
  "type": "sequence",
  "title": "Chat: sending a message",
  "spacing": 210,
  "numbered": true,
  "participants": [
    { "id": "c", "kind": "client", "label": "Sender app" },
    { "id": "gw", "kind": "service", "label": "Chat gateway" }
  ],
  "steps": [
    { "from": "c", "to": "gw", "label": "SEND msg (client_msg_id)" },
    { "from": "gw", "to": "c", "reply": true, "label": "ACK server_id" },
    { "note": "line one\nline two", "over": ["gw", "c"] },
    { "section": "alt: recipient offline" },
    { "from": "gw", "to": "gw", "label": "self call" }
  ]
}
```

Messages are numbered automatically unless `"numbered": false`; replies (`"reply": true`, dashed) are not
numbered. Use `"dashed": true` for an async message that is not a reply. A note spans the participants in
`over` (or the single one in `at`) and grows to fit its text; split long text with `\n`. A `section` draws
a full-width divider, useful for `alt:` and `loop:` blocks. `spacing` is the distance between lifelines.

## Checking your work

Render, then open the PNG. Things to look for: labels sitting on a box border, two lines drawn on top of
each other, an elbow crossing a box. Fix them with `offset`, `via` or `labelPos`, not by adding nodes.
