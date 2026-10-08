# Transcript · run run-transcript-1

_Legend: ✓ canonical mission event; · provider frame (evidence, not state)_

## Run

- ✓ <hh:mm:ss> · mission_control · workflow_run.admit  -> pending
- ✓ <hh:mm:ss> · mission_control · workflow_run.start pending -> active

## Activation <uuid> · collect

- · <hh:mm:ss> · system · session_init

### Turn 1

- ✓ <hh:mm:ss> · agent · session.turn_started turn 1
- · <hh:mm:ss> · system · turn_started
- · <hh:mm:ss> · agent · message_delta
- · <hh:mm:ss> · tool · tool_call_started pubmed_search [call-1]
  <details>
  <summary>call-1 · <digest></summary>

  ```json
  {"args":{"q":"rapamycin"},"name":"pubmed_search","tool_call_id":"call-1"}
  ```
  </details>
- · <hh:mm:ss> · tool · tool_call_completed pubmed_search [call-1] success
  <details>
  <summary>call-1 · <digest></summary>

  ```json
  {"content":"12 results","name":"pubmed_search","status":"success","tool_call_id":"call-1"}
  ```
  </details>
- ✓ <hh:mm:ss> · tool · tool_call.completed pubmed_search completed
  <details>
  <summary>call-1 · <digest></summary>

  ```json
  {"name":"pubmed_search","result_digest":"<digest>","status":"completed","tool_call_ref":"call-1"}
  ```
  </details>
- · <hh:mm:ss> · system · usage
  - usage: input 8, output 3, cost_micros unknown
- · <hh:mm:ss> · system · turn_ended

## Run

- ✓ <hh:mm:ss> · mission_control · artifact.registered [sources.json](mc://artifacts/biotech/run-transcript-1/sources.json)
- ✓ <hh:mm:ss> · human · human_task.resolved

## Activation <uuid> · collect

- ✓ <hh:mm:ss> · agent · session.turn_completed turn 1
  - usage: input 8, output 3, cost_micros unknown
- · <hh:mm:ss> · system · run_result finished
