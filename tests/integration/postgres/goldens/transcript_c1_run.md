# Transcript · run <run>

_Legend: ✓ canonical mission event; · provider frame (evidence, not state)_

## Run

- ✓ <hh:mm:ss> · mission_control · workflow_run.admitted
- ✓ <hh:mm:ss> · mission_control · workflow_run.start_requested
- ✓ <hh:mm:ss> · mission_control · workflow_run.start pending -> active

## Activation <uuid>

- · <hh:mm:ss> · system · session_init
- · <hh:mm:ss> · system · turn_started
- · <hh:mm:ss> · agent · message_delta
- · <hh:mm:ss> · tool · tool_call_started ls [call-1]
  <details>
  <summary>call-1 · <digest></summary>

  ```json
  {"args":{"path":"/"},"name":"ls","tool_call_id":"call-1"}
  ```
  </details>
- · <hh:mm:ss> · system · usage
  - usage: input 3, output 1, cost_micros unknown
- · <hh:mm:ss> · tool · tool_call_completed ls [call-1] success
  <details>
  <summary>call-1 · <digest></summary>

  ```json
  {"content":"a.txt","name":"ls","status":"success","tool_call_id":"call-1"}
  ```
  </details>
- · <hh:mm:ss> · system · usage
  - usage: input 5, output 2, cost_micros unknown
- · <hh:mm:ss> · system · turn_ended
- · <hh:mm:ss> · system · run_result finished
- ✓ <hh:mm:ss> · agent · session.started

### Turn 1

- ✓ <hh:mm:ss> · agent · session.turn_started turn 1
- ✓ <hh:mm:ss> · tool · tool_call.completed ls completed
  <details>
  <summary>call-1 · <digest></summary>

  ```json
  {"args_digest":null,"exit_code":null,"name":"ls","result_digest":"<digest>","status":"completed","tool_call_ref":"call-1","turn_ordinal":1}
  ```
  </details>
- ✓ <hh:mm:ss> · agent · session.turn_completed turn 1
  - usage: input 8, output 3, cost_micros unknown
- ✓ <hh:mm:ss> · mission_control · attempt.completed succeeded
- ✓ <hh:mm:ss> · agent · session.ended
