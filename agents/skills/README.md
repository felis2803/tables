# Agent Skills

Project-local skills for AI agents should live here.
Vendored external skills are also acceptable here when we intentionally pin them for local agent workflows.

Recommended pattern:

- one directory per skill;
- at least one `SKILL.md` file inside;
- optional `references/`, `templates/`, and `scripts/` next to it.

Typical skills for this project:

- `table-reduction`
- `pipeline-performance`
- `result-comparison`
- `run-audit`
- `skill-creator` (vendored from `anthropics/skills`, path `skills/skill-creator`, imported from `main` at `12ab35c2eb5668c95810e6a6066f40f4218adc39`)
