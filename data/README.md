# Data factory

`raw/` is immutable source material. `interim/` contains cleaning outputs, and
`processed/` contains versioned, analysis-ready datasets. Contents are ignored by
Git. Preserve data provenance, hashes, adjustment rules, symbol mappings, timezones,
and the timestamp when each value became knowable. Store licensed or sensitive data
only under the applicable provider terms.
