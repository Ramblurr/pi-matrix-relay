#!/usr/bin/env bash
set -euo pipefail

staged_prompt_paths=()
while IFS= read -r -d '' path; do
    staged_prompt_paths+=("${path}")
done < <(git diff --cached --name-only -z -- prompts/)

if ((${#staged_prompt_paths[@]} == 0)); then
    exit 0
fi

cat >&2 <<'EOF'
🚫 DO NOT COMMIT prompts/ FILES

prompts/ is intentionally ignored. It is a human-operator scratch space.
Agents may edit files there, but under no circumstances may an agent
put prompts/ files under source control unless the human operator directly and explicitly instructed that this commit must include prompts/ files.

This is not an invitation to decide that a prompts/ file is important. If you
were not directly told to commit a file under prompts/, stop and unstage it now.

Remove staged prompts/ paths:
  git restore --staged -- prompts/

For a force-added ignored file that should remain in the working tree:
  git rm --cached -- prompts/<file>

Blocked staged prompts/ paths:
EOF

for path in "${staged_prompt_paths[@]}"; do
    printf '  - %s\n' "${path}" >&2
done

exit 1
