"""Review-issue priorities, explanations and next actions."""

# ---------------------------------------------------------------------------
# Review issues: priority, explanation and next action per warning category
# ---------------------------------------------------------------------------
SCHEMA_VERSION = 2

ISSUE_GUIDE = {
    # category: (priority, why it matters, what to do next)
    "credential hygiene": ("high", "A credential is stored in the factory definition, so anyone who can read "
                           "the definition or its Git history can read it.",
                           "Move the secret to Key Vault or use a managed identity, then rotate it."),
    "input": ("high", "Part of the input could not be used, so this document may be missing objects.",
              "Fix or re-export the file and regenerate."),
    "unresolved reference": ("high", "An object is used but was not supplied, so its reads, writes or "
                             "target are unknown.",
                             "Export the missing object (or the whole factory) and regenerate."),
    "cycle": ("high", "Activities that depend on each other in a loop can never all run.",
              "Break the dependency loop in the pipeline."),
    "trigger state": ("medium", "The trigger is saved as not started, so its pipelines do not run on it.",
                      "Confirm this is intended; start the trigger or remove it."),
    "inactive activity": ("medium", "The activity is skipped at runtime.",
                          "Confirm it is meant to be off; remove it if it is no longer needed."),
    "data flow": ("medium", "Data-flow output that nothing consumes is dead logic or a missing connection.",
                  "Connect the branch to a sink or remove it."),
    "unreferenced resource": ("low", "Nothing in the supplied input uses this object. Other factories, "
                              "external callers or objects not supplied may still use it.",
                              "Review as a removal candidate; check other factories and callers first."),
    "no known invoker": ("low", "No trigger or pipeline here starts this pipeline.",
                         "Find the external caller (REST, Logic Apps, Synapse) or review it for removal."),
}
NOTE_GUIDE = {
    "dynamic target": "The table or path is built from parameters or expressions, so the object "
                      "shown is only what can be known before the run.",
    "opaque": "The work happens in code ADF cannot see (notebook, procedure, external call); "
              "inspect that code for what it reads and writes.",
    "resilience": "Copy and transform activities without retries fail on the first transient error.",
    "error path": "These activities run on failure or completion, not only on success.",
    "isolated entity": "Read or checked but not part of any data movement.",
}
