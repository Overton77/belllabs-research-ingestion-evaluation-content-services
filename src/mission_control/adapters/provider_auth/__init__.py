"""Provider auth preflight: route detection, status parsing, redaction and limit
classification for the lane profiles (MP-05). Implements
`application.execution.auth_admission.AuthPreflightPort`; never reads credential values,
never attempts a login, never calls a paid provider."""
