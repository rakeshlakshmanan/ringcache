# Decision notes from parallel tracks

While several tracks run at once, each writes its decisions to its own
file here rather than appending to `docs/design.md`. One file per track
means no two tracks ever edit the same lines.

`docs/design.md` stays the single record: its per-phase sections are
assembled from these files as each track lands.

One file per track, named for the branch, for example
`track-c-etcd-harness.md`. Use the same style as `docs/design.md`: what
was decided, and why, in prose.
