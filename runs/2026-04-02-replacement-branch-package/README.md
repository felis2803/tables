# Replacement Artifact Package

This directory stores the large raw JSON artifacts for the `n -> n - k` replacement experiments in split archives, so the branch stays pushable to GitHub.

Contents:

- `bit_gain.raw-json.tar.gz.part00`
- `bit_gain.raw-json.tar.gz.part01`
- `net_arity_gain.raw-json.tar.gz.part00`
- `net_arity_gain.raw-json.tar.gz.part01`
- `cap_relief.raw-json.tar.gz.part00`
- `cap_relief.raw-json.tar.gz.part01`

Each archive contains:

- `runs/2026-04-02-replacement-pipeline-compare/<metric>/`
- `runs/2026-04-02-replacement-pipeline-compare/inputs/<metric>.tables.json`

Reconstruction on PowerShell:

```powershell
$parts = Get-ChildItem runs/2026-04-02-replacement-branch-package/bit_gain.raw-json.tar.gz.part* | Sort-Object Name
$dest = 'runs/2026-04-02-replacement-branch-package/bit_gain.raw-json.tar.gz'
$out = [System.IO.File]::Create($dest)
try {
  foreach ($part in $parts) {
    $bytes = [System.IO.File]::ReadAllBytes($part.FullName)
    $out.Write($bytes, 0, $bytes.Length)
  }
} finally {
  $out.Dispose()
}
tar -xzf $dest -C .
```

SHA-256:

- `bit_gain.raw-json.tar.gz.part00`: `FA4321E6F86CC8F910F8942919BBE54BC67ACC2C3A8A905AAD0C04942C420843`
- `bit_gain.raw-json.tar.gz.part01`: `C5C5BCCF897FF7E44CB94515C321929F4EE2154552CBAD48891475A7E3E703EA`
- `net_arity_gain.raw-json.tar.gz.part00`: `3CF21F1A859475D5232F9F5214C4D8D12D43B1F7C5B0682E174167E72D8D98E6`
- `net_arity_gain.raw-json.tar.gz.part01`: `434A3E795F2D846E2D40DCA33DE5A0DF85FB80E44D7F4C7DFC7E960A64178BC3`
- `cap_relief.raw-json.tar.gz.part00`: `80F37BF77D5F51D125D2470768F6D18FCBD7FA7108ED04C800476FE6B34F908E`
- `cap_relief.raw-json.tar.gz.part01`: `24FE0242FA94251B8EA99569600000FE5F232B64789B11D0FEFC9B05B34D7634`
